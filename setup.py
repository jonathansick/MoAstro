#!/usr/bin/env python
# encoding: utf-8
#!/usr/bin/env python
# encoding: utf-8
from setuptools import setup
from sphinx.setup_command import BuildDoc


dependencies = """
sphinx
numpy
pymongo
"""

cmdclass = {'build_sphinx': BuildDoc}

setup(name='moastro',
    version='0.2',
    author='Jonathan Sick',
    author_email='jonathansick@mac.com',
    description='MongoDB framework for observational astronomers',
    license='BSD',
    install_requires=dependencies.split(),
    cmdclass=cmdclass,
    packages=['moastro']
)
