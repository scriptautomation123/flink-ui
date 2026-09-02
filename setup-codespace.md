```bash
sudo apt update && sudo apt install -y curl
```


source "$HOME/.sdkman/bin/sdkman-init.sh"

sdk selfupdate force

sdk list java | grep -e "-tem"
sdk install java 21.0.12+1.1-tem

sdk install maven



curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
[ -s "$NVM_DIR/bash_completion" ] && . "$NVM_DIR/bash_completion"

nvm install --lts && \
nvm use --lts && \
nvm alias default 'lts/*'

node -v && \
npm -v && \
nvm current