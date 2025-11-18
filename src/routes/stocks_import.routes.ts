import { Router, text } from 'express';
import multer from 'multer';
import { requireAuth, requireAnyRole } from '../middlewares/auth';
import {
  importOpeningStocks,
  importOpeningOneFile,
} from '../services/stocks_import.service';

const router = Router();

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB
});

type ImportMode = 'replace' | 'add';

/** JSON rows */
router.post(
  '/opening',
  requireAuth,
  requireAnyRole(['admin', 'accountant']),
  async (req, res) => {
    try {
      const result = await importOpeningStocks(undefined as any, req.body);
      return res.json(Object.assign({ ok: true }, result));
    } catch (e: any) {
      return res.status(400).json({ ok: false, message: e?.message || 'Import failed' });
    }
  }
);

/** ONE FILE – chấp nhận mọi field file (upload.any) */
router.post(
  '/opening-onefile',
  requireAuth,
  requireAnyRole(['admin', 'accountant']),
  upload.any(),
  async (req, res) => {
    try {
      // Lấy file đầu tiên dù tên field là gì
      const files = (req as any).files as Express.Multer.File[] | undefined;
      const f =
        (files && files.find(ff => ff.fieldname === 'file')) ||
        (files && files[0]);
      if (!f) {
        return res.status(400).json({
          ok: false,
          message: "Missing file — please upload with key 'file'",
        });
      }

      const rawMode = (req.body?.mode ?? 'replace').toString().toLowerCase();
      const mode: ImportMode = rawMode === 'add' || rawMode === 'adjust' ? 'add' : 'replace';

      const result = await importOpeningOneFile(f.buffer, { mode });
      return res.json(Object.assign({ ok: true }, result));
    } catch (e: any) {
      return res.status(400).json({ ok: false, message: e?.message || 'Import failed' });
    }
  }
);

/** CSV raw (tuỳ chọn) */
router.post(
  '/opening/csv',
  requireAuth,
  requireAnyRole(['admin', 'accountant']),
  text({ type: ['text/csv', 'application/vnd.ms-excel', 'text/plain'] }),
  async (req, res) => {
    try {
      const csv = req.body || '';
      if (!csv.trim()) return res.status(400).json({ ok: false, message: 'Empty CSV body' });

      const rawMode = (req.query.mode || req.body?.mode || 'replace').toString().toLowerCase();
      const mode: ImportMode = rawMode === 'add' || rawMode === 'adjust' ? 'add' : 'replace';

      const buf = Buffer.from(csv, 'utf8');
      const result = await importOpeningOneFile(buf, { mode });
      return res.json(Object.assign({ ok: true }, result));
    } catch (e: any) {
      return res.status(400).json({ ok: false, message: e?.message || 'Import failed' });
    }
  }
);

export default router;
