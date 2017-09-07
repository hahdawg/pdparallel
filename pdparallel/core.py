import multiprocessing as mp
import pandas as pd


def parallel_apply(grouped, func, func_args=None, chunksize=None, maxtasksperchild=None, njobs=-1):
    """
    Does a pandas apply in parallel.

    Parameters
    ----------
    grouped: result of df.groupby()
    func: DataFrame -> DataFrame
    func_args: tuple
    chunksize: int
        See documentation for multiprocessing.imap_unordered
    maxtasksperchild: int
        See documentation for multiprocessing.pool
    njobs:
        Number of cores to use. Set to -1 to use all cores.

    Returns
    -------
    df.groupby([xs], as_index=False).apply(func)
    """
    if njobs == -1:
        njobs = mp.cpu_count()

    chunksize = chunksize or 1
    pool = mp.Pool(processes=njobs, maxtasksperchild=maxtasksperchild)
    if func_args is None:
        chunks = (chunk for _, chunk in grouped)
    else:
        chunks = ((chunk,) + func_args for _, chunk in grouped)

    res = pool.imap_unordered(func, chunks, chunksize=chunksize)
    pool.close()
    pool.join()
    res = list(res)
    if isinstance(res[0], dict):
        res = pd.DataFrame(res)
    else:
        res = pd.concat(res, axis=0)
    return res
