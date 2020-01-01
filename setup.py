from setuptools import setup

setup(name='orchard_aws',
      url='https://github.com/Orchard-Analytics/orchard_aws',
      packages=['orchard_aws'],
      version='0.25',
      description='Orchad Analytics package for interacting with AWS.',
      author='Ryan Brennan',
      author_email='ryantbrennan1@gmail.com',
      python_requires='>=3',
      install_requires=['pandas', 'numpy', 'psycopg2-binary', 'boto3', 'pyyaml'],
      include_package_data=True)