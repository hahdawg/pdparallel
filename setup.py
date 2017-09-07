from setuptools import setup, find_packages


setup(name="pdparallel",
      version="0.1",
      author="andrew hah",
      author_email="hahdawg@yahoo.com",
      packages=find_packages(),
      install_requires=[
          "pandas"
      ],
      zip_safe=False
      )
