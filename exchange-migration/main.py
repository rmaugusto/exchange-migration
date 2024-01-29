from emails import EmailMigrator
from yaml import load, Loader

def main():

    config = load(open('config.yaml', 'r'), Loader=Loader)

    accounts = config['accounts']

    for account_idx in range(0, len(accounts)):
        em = EmailMigrator()
        em.run(config, account_idx)

if __name__ == "__main__":
    main()
