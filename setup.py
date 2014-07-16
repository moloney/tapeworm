from setuptools import setup

#Hard dependencies
install_requires = ['pathmap>=2.0a2', 
                    'peewee',
                   ]

setup(name='tapeworm',
      description='Manage backups to tape library on Linux',
      version='0.1-dev',
      author='Brendan Moloney',
      author_email='moloney@ohsu.edu',
      packages=['tapeworm'],
      install_requires=install_requires,
      entry_points = {'console_scripts' : \
                          ['tapeworm = tapeworm.tapeworm:main'],
                     },
     )
