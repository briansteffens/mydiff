from mydiff import config, __compare

if __name__ == '__main__':
    for change in __compare(config('config.json')):
        print(change)
