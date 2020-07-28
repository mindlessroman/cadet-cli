from setuptools import setup

setup(
    name='cadet',
    version='1.1.0',
    py_modules=['cadet'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        cadet=cadet:upload
    ''',
)