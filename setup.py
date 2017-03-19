from setuptools import setup
setup(
  name = 'btrdb4',
  packages = ['btrdb4'],
  version = '0.0.1',
  description = "Bindings to interact with the Berkeley Tree Database using gRPC",
  author = 'Sam Kumar, Michael P Andersen',
  author_email = 'samkumar99@gmail.com, michael@steelcode.com',
  url = 'https://github.com/SoftwareDefinedBuildings/btrdb4-python',
  download_url = 'https://github.com/SoftwareDefinedBuildings/btrdb4-python/tarball/1.0',
  package_data = { 'btrdb4': ['grpcinterface/btrdb.proto'] },
  include_package_data = False,
  install_requires = [],
  keywords = ['btrdb', 'berkeley', 'timeseries', 'database', 'bindings' 'gRPC'],
  classifiers = []
)
