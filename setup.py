from setuptools import setup

setup(
    name='gazprom_ingest',
    entry_points={
        'console_scripts': [
            'gazprom_ingest = gazprom_ingest:main',
        ],
    },
    use_2to3=True,
)