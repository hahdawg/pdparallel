import multiprocessing as mp
import pandas as pd


def parallel_apply(grouped, func, func_args=None, pool_kwargs=None,
                   imap_kwargs=None, njobs=-1):
    """
    Does a pandas apply in parallel.

    Parameters
    ----------
    grouped: result of df.groupby()
    func: DataFrame -> DataFrame
    func_args: tuple
    pool_kwargs: dict
        Parameters to pass to multiprocessing.Pool
    imap_kwargs: dict
        Parameters to pass to multiprocessing.Pool.imap_unordered
    njobs:
        Number of cores to use. Set to -1 to use all cores.

    Returns
    -------
    df.groupby([xs], as_index=False).apply(func)
    """
    if njobs == -1:
        njobs = mp.cpu_count()

    pool_kwargs = pool_kwargs or dict()
    imap_kwargs = imap_kwargs or dict()

    pool = mp.Pool(processes=njobs, **pool_kwargs)
    if func_args is None:
        chunks = (chunk for _, chunk in grouped)
    else:
        chunks = ((chunk,) + func_args for _, chunk in grouped)

    applied = pool.imap_unordered(func, chunks, **imap_kwargs)
    pool.close()
    pool.join()

    applied = list(applied)
    if isinstance(applied[0], dict):
        applied = pd.DataFrame(applied)
    else:
        applied = pd.concat(applied, axis=0)
    return applied
