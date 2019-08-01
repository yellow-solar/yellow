#### Create virtualenvironemnt in in your home directory 
virtualenv env_name

Activate: source env_name/bin/activate
Deactivate: deactivate

## Sometimes installing from cache doesnt work, to isntall libraries by downloading fresh
pip install <name> --no-cache-dir

Create an alias so you can activate the venv quickly by adding something like this to your .bashrc
format:
alias aliasname='command'

#aliases to activate virtualenv
alias activ_nff='source ~/Projects/.venv/newfeefee/bin/activate'

# install requirements.txt from project root after ativating the virtualenv
pip install -r requirements.txt

# install mysqlclient if failed
sudo apt-get install install libssl-dev
sudo yum install libssl-dev
pip install mysqlclient



