from setuptools import setup

setup(name='orchard_aws',
      url='https://github.com/Orchard-Analytics/orchard-aws',
      packages=['orchard_aws'],
      version='0.1',
      description='Orchad Analytics package for interacting with AWS.',
      author='Ryan Brennan',
      author_email='ryantbrennan1@gmail.com',
      python_requires='>=3',
      install_requires=['pandas', 'numpy', 'psycopg2-binary', 'boto3'],
      include_package_data=True)