import { Request, Response, NextFunction } from 'express';
import { JwtUser } from './auth';

/**
 * Cho phép:
 *  - Các role truyền vào (accountant/admin...) đi qua thẳng.
 *  - Staff thì chỉ được khi userId mục tiêu === chính họ
 *    (đọc từ query/body/params).
 */
export function requireSelfOrRole(...roles: JwtUser['role'][]) {
  return (req: Request, res: Response, next: NextFunction) => {
    const user = (req as any).user as JwtUser | undefined;
    if (!user) return res.status(401).json({ message: 'Unauthorized' });

    if (roles.includes(user.role)) return next();

    const targetUserId =
      (req.query.userId as string) ||
      (req.body?.userId as string) ||
      (req.params?.userId as string);

    if (user.role === 'staff' && targetUserId && targetUserId === user.id) {
      return next();
    }
    return res.status(403).json({ message: 'Forbidden' });
  };
}
