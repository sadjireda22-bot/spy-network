FROM node:22-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY server.js index.html README.md ./
ENV PORT=3000
ENV DATA_DIR=/data
EXPOSE 3000
CMD ["npm", "start"]
