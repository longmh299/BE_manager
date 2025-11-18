import { Router } from 'express';
import { requireAuth, requireRole } from '../middlewares/auth';
import { listLocations, createLocation, updateLocation, removeLocation } from '../services/locations.service';

const r = Router();
r.use(requireAuth);

/** GET /locations */
r.get('/', async (_req, res, next) => {
  try {
    const data = await listLocations();
    res.json({ ok: true, data });
  } catch (e) { next(e); }
});

/** POST /locations (admin) */
r.post('/', requireRole('admin'), async (req, res, next) => {
  try {
    const created = await createLocation(req.body);
    res.json({ ok: true, data: created });
  } catch (e) { next(e); }
});

/** PUT /locations/:id (admin) */
r.put('/:id', requireRole('admin'), async (req, res, next) => {
  try {
    const updated = await updateLocation(req.params.id, req.body);
    res.json({ ok: true, data: updated });
  } catch (e) { next(e); }
});

/** DELETE /locations/:id (admin) */
r.delete('/:id', requireRole('admin'), async (req, res, next) => {
  try {
    const del = await removeLocation(req.params.id);
    res.json({ ok: true, data: del });
  } catch (e) { next(e); }
});

export default r;
