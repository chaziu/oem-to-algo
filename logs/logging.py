import logging


def load_log_setting(log_file):
    logging.basicConfig(filename='./logs/'+log_file, level=logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    logging.getLogger().addHandler(ch)
