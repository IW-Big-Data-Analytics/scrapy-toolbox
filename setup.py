import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="scrapy-toolbox",
    version="0.1.0",
    author="Jan Wendt",
    description="Saves Scrapy exceptions in your Database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/janwendt/scrapy-toolbox",
    download_url="https://github.com/janwendt/scrapy-toolbox/archive/0.1.0.tar.gz",
    packages=setuptools.find_packages(),
    entry_points = {
        "console_scripts": ["scrapy-toolbox=scrapy_toolbox.command_line:main"],
    },
    install_requires=[
          "scrapy",
          "sqlalchemy",
          "sqlalchemy_utils"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)