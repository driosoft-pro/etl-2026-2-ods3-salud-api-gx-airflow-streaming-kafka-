import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


if __name__ == '__main__':
    args = [
        os.path.dirname(__file__),
        '-v',
        '--tb=short',
        '-m', 'not integration'
    ]
    
    if '--integration' in sys.argv:
        args = [
            os.path.dirname(__file__),
            '-v',
            '--tb=short',
            '-m', 'integration'
        ]
    
    if '--all' in sys.argv:
        args = [
            os.path.dirname(__file__),
            '-v',
            '--tb=short'
        ]
    
    sys.exit(pytest.main(args))