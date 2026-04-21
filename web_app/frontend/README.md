# Next.js Frontend

Minimal product frontend for the first HarnessMate user flow:

- Search page with PN, cavity count, plug/receptacle, and pin/socket filters
- Part detail page with compatibility, hardware, wire, and engineering sections
- Grouped mate results with mating PN, match reasons, and known hardware compatibility

Setup:

```powershell
cd web_app/frontend
copy .env.example .env.local
npm install
npm run dev
```

Required environment variable:

- `HARNESSMATE_API_BASE_URL=http://127.0.0.1:8000`

This frontend intentionally avoids:

- auth
- chat
- BOM builder
- complex client state
- design-system work
