import logging


# Using Root logger to log for both File & Console
def load_log_setting(log_file: str) -> None:
    logging.basicConfig(filename='./logs/'+log_file, level=logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    logging.getLogger().addHandler(ch)
