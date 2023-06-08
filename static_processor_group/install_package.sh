#!/bin/bash
cd ~/Sunbird-cQube-processing-ms/impl/c-qube
yarn install
npx prisma generate
npx prisma migrate deploy