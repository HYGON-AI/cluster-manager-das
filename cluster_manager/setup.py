# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
import atexit
import shutil
import sys
from pathlib import Path

import setuptools


def _stage_repository_legal_files():
    """Expose repository legal files to setuptools without tracking copies."""
    project_dir = Path(__file__).resolve().parent
    repository_root = project_dir.parent
    created_files = []

    for filename in ("LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md"):
        target = project_dir / filename
        if target.exists():
            continue

        source = repository_root / filename
        if not source.is_file():
            raise FileNotFoundError(f"Required repository legal file not found: {source}")
        shutil.copy2(source, target)
        created_files.append(target)

    def cleanup():
        for path in created_files:
            path.unlink(missing_ok=True)

    atexit.register(cleanup)


_stage_repository_legal_files()

if not (3, 10) <= sys.version_info < (3, 13):
    raise Exception("Python >=3.10,<3.13 is required by hcu-cluster-inspect.")

__description__ = 'HCU智韧集群检查监控工具：提供节点健康检查、分布式任务监控、异常告警'
__version__ = '1.0.0'
__author__ = 'HYGON-AI'
__contact_names__ = 'HYGON-AI'
__long_description__ = ''
__keywords__ = 'cluster_manager, hcu, cluster, monitor, inspect'
__package_name__ = 'hcu-cluster-inspect'

try:
    with open("README.md", "r", encoding="utf8") as fh:
        __long_description__ = fh.read()
except FileNotFoundError:
    pass

setuptools.setup(
    name=__package_name__,
    version=__version__,
    description=__description__,
    long_description=__long_description__,
    long_description_content_type="text/markdown",
    author=__contact_names__,
    maintainer=__contact_names__,
    license="Apache-2.0",
    license_files=("LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md"),
    classifiers=[
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Environment :: Console',
        'Natural Language :: Chinese',
        'Operating System :: OS Independent',
        'Intended Audience :: System Administrators',
        'Topic :: System :: Monitoring',
        'Topic :: System :: Clustering',
    ],
    python_requires='>=3.10,<3.13',

    packages=setuptools.find_packages(exclude=("test", "test.*")),

    include_package_data=True,
    zip_safe=False,
    keywords=__keywords__,
    cmdclass={},
    ext_modules=[],
    install_requires=[
        "numpy>=1.24",
        "pytz>=2023.3",
        "PyYAML>=6.0",
        "requests>=2.31",
        "torch>=2.4.0",
        "urllib3>=1.26",
    ],

    entry_points={
        "console_scripts": [
            "hcu-cluster-inspect=cluster_manager.main:main",
        ]
    }
)
