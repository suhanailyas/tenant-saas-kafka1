// Vercel Serverless Function: async invoice worker + cold/warm start detection
let isWarm = false;           // module scope survives between warm invocations
const bootTime = Date.now();

module.exports = function handler(req, res) {
  const start = Date.now();
  const startType = isWarm ? 'WARM' : 'COLD';
  isWarm = true;

  const orderId = req.query.orderId || 'demo';
  // simulate invoice generation work
  let x = 0; for (let i = 0; i < 2e6; i++) x += i;

  res.status(200).json({
    invoice: `INV-${orderId}`,
    startType,
    containerAgeMs: Date.now() - bootTime,
    executionMs: Date.now() - start,
  });
}
