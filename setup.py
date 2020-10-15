import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="scrapy-toolbox",
    version="0.0.1",
    author="Jan Wendt",
    description="Saves Scrapy exceptions in your Database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/janwendt/scrapy-toolbox",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)