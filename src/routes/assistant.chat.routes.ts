// src/routes/assistant.chat.routes.ts
import { Router } from "express";
import { requireAuth, getUser } from "../middlewares/auth";
import { runAssistantChat } from "../services/assistant/chatService";

const router = Router();

// POST /api/assistant/chat
// body: { message: string, history?: {role: "user"|"assistant", content: string}[] }
router.post("/chat", requireAuth, async (req, res, next) => {
  try {
    const { message, history } = req.body || {};

    if (!message || typeof message !== "string" || !message.trim()) {
      return res.status(400).json({ ok: false, message: "Thiếu nội dung câu hỏi (message)" });
    }

    const role = getUser(req)?.role;

    const { reply } = await runAssistantChat({
      userMessage: message.trim(),
      history: Array.isArray(history) ? history : [],
      role,
    });

    res.json({ ok: true, reply });
  } catch (e) {
    next(e);
  }
});

export default router;