FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY tsconfig.json ./
COPY src/ ./src/
COPY tests/ ./tests/
COPY schema.sql ./

RUN npm run build

EXPOSE 4000
CMD ["npm", "run", "start:api"]