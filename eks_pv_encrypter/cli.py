"""Simple Command Line Interface.
"""

import fire

from eks_pv_encrypter.main import collect_info


def main():
    fire.Fire({"status": collect_info})


if __name__ == "__main__":
    main()
