// src/app.ts
import express, { Router } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import morgan from 'morgan';

import { authRouter } from './routes/auth.routes';
import itemRoutes from './routes/items.routes';
import locationRoutes from './routes/locations.routes';
import movementRoutes from './routes/movements.routes';
import stockCountRoutes from './routes/stockcounts.routes';
import stockRoutes from './routes/stocks.routes';
import assetRoutes from './routes/assets.routes';
import invoiceRoutes from './routes/invoices.routes';
import stockImportRoutes from './routes/stocks_import.routes';
import partnersRoutes from './routes/partners.routes';
import { reportsRouter } from './routes/reports.routes';
import usersRoutes from './routes/users.routes';
import machinesRoutes from './routes/machines.routes';
import locksRoutes from './routes/locks.routes';

const app = express();

// ================== CORS CONFIG (đơn giản) ==================
const allowedOrigins = [
  'http://localhost:5173',
  'https://apibrother.id.vn',
  'https://www.apibrother.id.vn',
];

app.use(
  cors({
    origin(origin, callback) {
      // Cho Postman/curl (không có Origin)
      if (!origin) return callback(null, true);

      if (allowedOrigins.includes(origin)) {
        return callback(null, true);
      }

      console.warn('Blocked by CORS:', origin);
      return callback(new Error('Not allowed by CORS'));
    },
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: false, // bạn dùng Bearer token, không dùng cookie
  })
);
// ================== END CORS CONFIG ==================

// Middlewares khác
app.use(helmet());
app.use(express.json());
app.use(morgan('dev'));

// Health (không prefix để k8s/load balancer probe dễ gọi)
app.get('/health', (_req, res) => res.json({ ok: true }));
app.get('/api/health', (_req, res) => res.json({ ok: true })); // tùy thích giữ thêm

// Debug: log sớm
app.use((req, _res, next) => {
  console.log('➡', req.method, req.url);
  next();
});

// ==== API v1 (prefix /api) ====
const api = Router();

// đặt tất cả router vào /api cho đồng bộ
api.use('/auth', authRouter);
api.use('/items', itemRoutes);
api.use('/locations', locationRoutes);
api.use('/movements', movementRoutes);
api.use('/stock-counts', stockCountRoutes);
api.use('/stocks', stockRoutes);
api.use('/assets', assetRoutes);
api.use('/invoices', invoiceRoutes);
api.use('/imports/stocks', stockImportRoutes);
api.use('/partners', partnersRoutes);
api.use('/reports', reportsRouter);
api.use('/locks', locksRoutes);

// 2 router này bạn đang mount trực tiếp, giữ nguyên
app.use('/api/users', usersRoutes);
app.use('/api/machines', machinesRoutes);

// Mount đúng 1 lần dưới /api
app.use('/api', api);

// Debug: liệt kê routes (dev only)
function listRoutes(router: any) {
  const rts: string[] = [];
  router.stack?.forEach((l: any) => {
    if (l.route?.path) {
      const methods = Object.keys(l.route.methods).join(',').toUpperCase();
      rts.push(`${methods} ${l.route.path}`);
    } else if (l.name === 'router' && l.handle?.stack) {
      l.handle.stack.forEach((h: any) => {
        if (h.route) {
          const m = Object.keys(h.route.methods).join(',').toUpperCase();
          rts.push(`${m} ${h.route.path}`);
        }
      });
    }
  });
  return rts;
}

app.get('/api/__routes', (_req, res) =>
  res.json({ base: listRoutes(app), api: listRoutes(api) })
);

// Errors & 404
app.use((err: any, _req: any, res: any, _next: any) => {
  console.error(err);
  res.status(err.status || 500).json({
    ok: false,
    message: err.message || 'Server error',
  });
});

app.use((_req, res) =>
  res.status(404).json({ ok: false, message: 'Not found' })
);

export default app;
