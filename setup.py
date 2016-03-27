from setuptools import setup
setup(name='PyShelf',
      version='0.1',
      description='Shelf: Interactive small data',
      author='Morgan Stuart',
      install_requires=["requests", "flask-restful", "flask", "whoosh"],
      #packages=['PyShelf', ])
      packages=['PyShelf', 
          'PyShelf.common', 'PyShelf.index_descriptions', 
          'PyShelf.resources', 'PyShelf.util'])
