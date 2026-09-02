FROM node:26-alpine
WORKDIR /app
COPY package.json game.js server.js ./
COPY public ./public
ENV NODE_ENV=production PORT=3000
EXPOSE 3000
CMD ["node", "server.js"]
