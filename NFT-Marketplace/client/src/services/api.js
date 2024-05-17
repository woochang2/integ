import axios from 'axios';

export const api = axios.create({
  baseURL: process.env.API_URL || 'http://141.223.121.131:3333/'
});