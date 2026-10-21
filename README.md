# wigos-node

## setup dev environment 

- need docker, docker compose and pyenv 

pyenv install 3.12
pyenv local 3.12
create venv with vs code, install dependencies from requirements.txt
go

## frontend

Vue 3 + Vuetify 4, built with Vite, in `frontend/`. The dev server runs in the
`frontend` container on http://localhost:5173 and proxies `/api` to the backend.

Node isn't needed on the host; run npm in the container:

    docker compose run --rm --no-deps frontend npm test
    docker compose run --rm --no-deps frontend npm run build

After changing dependencies, rebuild the image and renew its `node_modules`
volume:

    docker compose up -d --build -V frontend