function fib(n) { return n < 2 ? n : fib(n-1) + fib(n-2); }
function spin(n) { let x = 0; for (let i = 0; i < n; i++) x = (x + (i%7)) | 0; return x; }
function hot() { fib(30); spin(3000000); }
hot(); hot();
