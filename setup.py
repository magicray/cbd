import time
from distutils.core import setup

setup(
  name='cbd',
  py_modules=['cbd'],
  version=time.strftime('%Y%m%d'),
  description='Network Block Device, backed by S3 like Object Store',
  long_description='Network Block Device, backed by S3 like Object Store',
  author='Bhupendra Singh',
  author_email='bhsingh@gmail.com',
  url='https://github.com/magicray/cbd',
  keywords=['nbd', 'linux', 's3', 'network', 'block', 'device']
)
