import { Router } from "express";
import { requireAuth } from "../middlewares/auth";
import {
  getCustomers,
  getCustomerDetail,
  updateCustomer, // 🔥 ADD
} from "../services/customers.service";
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

const r = Router();

r.use(requireAuth);

/**
 * ===============================
 * GET /api/customers (LIST)
 * ===============================
 */
r.get("/", async (req: any, res, next) => {
  try {
    const userId = req.user?.id;

    const onlyMine = req.query.onlyMine !== "false";
    const page = Number(req.query.page) || 1;
    const pageSize = Number(req.query.pageSize) || 20;

    const data = await getCustomers({
      userId,
      onlyMine,
      page,
      pageSize,
    });

    res.json({
      ok: true,
      data,
      page,
      pageSize,
    });
  } catch (e) {
    next(e);
  }
});

/**
 * ===============================
 * GET /api/customers/:id (DETAIL)
 * ===============================
 */
r.get("/:id", async (req, res, next) => {
  try {
    const id = req.params.id;

    const data = await getCustomerDetail(id);

    if (!data) {
      return res.status(404).json({
        ok: false,
        message: "Customer not found",
      });
    }

    res.json({
      ok: true,
      data,
    });
  } catch (e) {
    next(e);
  }
});

/**
 * ===============================
 * 🔥 PUT /api/customers/:id (UPDATE)
 * ===============================
 */
r.put("/:id", async (req, res, next) => {
  try {
    const id = req.params.id;

    const { phone, email, taxCode, address, name } = req.body;

    // 👉 validate cơ bản (optional nhưng nên có)
    if (
      phone === undefined &&
      email === undefined &&
      taxCode === undefined &&
      address === undefined &&
      name === undefined
    ) {
      return res.status(400).json({
        ok: false,
        message: "No data to update",
      });
    }

    const updated = await updateCustomer(id, {
      phone,
      email,
      taxCode,
      address,
      name,
    });

    res.json({
      ok: true,
      data: updated,
    });
  } catch (e) {
    console.error(e);

    res.status(500).json({
      ok: false,
      message: "Update failed",
    });
  }
});

/**
 * ===============================
 * POST /api/customers/:id/activity
 * ===============================
 */
r.post("/:id/activity", async (req: any, res, next) => {
  try {
    const userId = req.user?.id;
    const partnerId = req.params.id;

    const { type, content } = req.body;

    if (!type) {
      return res.status(400).json({
        ok: false,
        message: "type is required",
      });
    }

    const activity = await prisma.customerActivity.create({
      data: {
        partnerId,
        type,
        content,
        createdBy: userId,
      },
    });

    res.json({
      ok: true,
      data: activity,
    });
  } catch (e) {
    next(e);
  }
});

export default r;