from setuptools import setup, find_packages

setup(
    name="aclimate_resampling",
    version='v1.0.0',
    author="stevensotelo",
    author_email="h.sotelo@cgiar.com",
    description="Resampling module",
    url="https://github.com/CIAT-DAPA/aclimate_resampling",
    download_url="https://github.com/CIAT-DAPA/aclimate_resampling",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    keywords='resampling aclimate',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "mongoengine==0.26.0"
    ]
)