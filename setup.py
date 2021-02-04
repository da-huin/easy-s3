
# Use this command for deploy.
#   python3 setup.py sdist bdist_wheel && python3 -m twine upload --skip-existing dist/*

import io
from setuptools import find_packages, setup

setup(name='easy_s3',
      version='1.0.7',
      description='This package helps you use S3 easily.',
      long_description="Please refer to the https://github.com/da-huin/easy_s3",
      long_description_content_type="text/markdown",
      url='https://github.com/da-huin/easy_s3',
      download_url= 'https://github.com/da-huin/easy_s3/archive/master.zip',
      author='JunYeong Park',
      author_email='dahuin000@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=["boto3"],
      classifiers=[
          'Programming Language :: Python :: 3',
    ]
)


