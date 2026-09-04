from setuptools import find_packages, setup

setup(
    name="pyspark-ecommerce-quality-pipeline",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["pyspark>=3.5,<4.0"],
    extras_require={"dev": ["pytest>=8.0", "chispa>=0.10.0"]},
)
