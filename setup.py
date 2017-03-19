from setuptools import setup
setup(
  name = 'btrdb4',
  packages = ['btrdb4'],
  version = '4.4.5',
  description = "Bindings to interact with the Berkeley Tree Database using gRPC",
  author = 'Sam Kumar, Michael P Andersen',
  author_email = 'samkumar99@gmail.com, michael@steelcode.com',
  url = 'https://github.com/SoftwareDefinedBuildings/btrdb4-python',
  download_url = 'https://github.com/SoftwareDefinedBuildings/btrdb4-python/tarball/0.0.1',
  package_data = { 'btrdb4': ['grpcinterface/btrdb.proto'] },
  include_package_data = False,
  install_requires = ["grpcio >= 1.1.3", "grpcio-tools >= 1.1.3"],
  keywords = ['btrdb', 'berkeley', 'timeseries', 'database', 'bindings' 'gRPC'],
  classifiers = []
)
