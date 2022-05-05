#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
    th-e-fcst
    ~~~~~~~~~
    
    To learn how to configure the prediction of timeseries, see "th-e-fcst --help"

"""
import os
import shutil

from argparse import ArgumentParser, RawTextHelpFormatter
from configparser import ConfigParser


def main(args):
    from th_e_core import System, configs

    settings = configs.read('settings.cfg', **vars(args))

    kwargs = vars(args)
    kwargs.update(settings.items('General'))

    systems = System.read(**kwargs)
    if args.action == 'build':
        systems.build(**kwargs)


def _get_parser(root_dir):
    from th_e_core import __version__
    
    parser = ArgumentParser(description=__doc__, formatter_class=RawTextHelpFormatter)
    parser.add_argument('-v', '--version',
                        action='version',
                        version='%(prog)s {version}'.format(version=__version__))
    
    subparsers = parser.add_subparsers(dest='action')
    subparsers.required = True
    subparsers.add_parser('build', help='Build data for the configured set of systems')
    
    parser.add_argument('-r', '--root-directory',
                        dest='root_dir',
                        help="directory where the package and related libraries are located",
                        default=root_dir,
                        metavar='DIR')
    
    parser.add_argument('-c', '--config-directory',
                        dest='config_dir',
                        help="directory to expect configuration files",
                        default='conf',
                        metavar='DIR')
    
    parser.add_argument('-d', '--data-directory',
                        dest='data_dir',
                        help="directory to expect and write result files to",
                        default='data',
                        metavar='DIR')
    
    return parser


if __name__ == "__main__":
    run_dir = os.getcwd()
    if os.path.basename(run_dir) == 'bin':
        run_dir = os.path.dirname(run_dir)

    os.chdir(run_dir)

    os.environ['NUMEXPR_MAX_THREADS'] = str(os.cpu_count())

    if not os.path.exists('log'):
        os.makedirs('log')

    logging_file = os.path.join(os.path.join(run_dir, 'conf'), 'logging.cfg')
    if not os.path.isfile(logging_file):
        logging_default = logging_file.replace('logging.cfg', 'logging.default.cfg')
        if os.path.isfile(logging_default):
            shutil.copy(logging_default, logging_file)
        else:
            raise FileNotFoundError("Unable to open logging.cfg in: " +
                                    os.path.join(os.path.join(run_dir, 'conf')))

    # Load the logging configuration
    import logging
    import logging.config
    logging.config.fileConfig(logging_file)
    logger = logging.getLogger('th-e-data')

    main(_get_parser(run_dir).parse_args())
