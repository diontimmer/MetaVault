from setuptools import setup

setup(
    name="MetaVault",
    version="0.3",
    download_url="https://github.com/diontimmer/MetaVault/archive/refs/tags/v_0.3.tar.gz",
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
