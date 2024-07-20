from setuptools import setup

setup(
    name="MetaVault",
    version="0.1",
    packages=[""],
    license="MIT",
    author="Dion Timmer",
    author_email="diontimmer@live.nl",
    description="A simple database for storing metadata associated with (media) files.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    scripts=["metavault.py"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
