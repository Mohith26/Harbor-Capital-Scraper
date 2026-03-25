# Harbor Capital Comp Database

A tool for managing commercial real estate (CRE) comparable properties — upload messy spreadsheets, and the app cleans, geocodes, and organizes everything into a searchable database with analytics.

---

## Access

**Live App:** https://harbor-capital-scraper-production.up.railway.app/

**GitHub:** https://github.com/Mohith26/Harbor-Capital-Scraper

### Logins

| Name | Username | Password | Role |
|------|----------|----------|------|
| Isaac Specter | isaac@harborcap.com | blueforest | Admin |
| Chad Gustenhoven | chad@harborcap.com | goldensun | Analyst |
| Will Ghiselli | will@harborcap.com | quietlake | Analyst |
| Trevor Allison | trevor@harborcap.com | redmountain | Analyst |
| Steve Naidu | steve@harborcap.com | silvercloud | Analyst |
| Hahn Franklin-Mitchell | hahn@harborcap.com | greenfield | Analyst |
| Harris Quiner | harris@harborcap.com | softbreeze | Analyst |
| (Generic Admin) | admin | harbor2024 | Admin |
| (Generic Analyst) | analyst | harbor2024 | Analyst |

**Admin** = full access — upload, edit database records, delete, clear database
**Analyst** = upload, view, filter, export, edit data before saving — cannot edit or delete records already in the database

---

## What It Does

### Upload & Process
Drop in an Excel or CSV file and the app uses AI (OpenAI) to automatically figure out which columns map to which fields (address, price, rate, size, etc.). Then it geocodes all the addresses through Google Maps. Review everything before saving to the database. Duplicates are auto-detected and skipped.

### Database View
Browse all your sales and lease comps in one place. Filter by city, zip, price range, size, building type, date — whatever you need. There's a map view with clustered markers, proximity search (find everything within X miles of an address), and you can export filtered results as CSV, Excel, or KML.

### Analytics
Charts and stats for your comp data — price distributions, $/SF trends over time, scatter plots with trendlines, zip code breakdowns, a geographic heat map, and a side-by-side property comparison tool. All filterable.

### Comp Finder
The newest feature. Enter details about a subject property (address, size, price, etc.) and it ranks every comp in your database by how similar it is. You control the weights — prioritize proximity, size, price, or recency. There's an optional AI mode that uses OpenAI embeddings for smarter semantic matching. Results show up as a ranked table with match percentages, an interactive map with color-coded markers, and a score breakdown chart.

---

## Tech Stack
Streamlit + Supabase (PostgreSQL) + OpenAI + Google Maps API, deployed on Railway.
