import pdfplumber
import polars as pl
import re

PDF_PATH = "../Livrable3/DC_DL_RPG_2-0.pdf"
OUTPUT_PATH = "RPG_annexe_A.xlsx"

tables_found = []

with pdfplumber.open(PDF_PATH) as pdf:
    in_annexe_a = False

    for i, page in enumerate(pdf.pages):
        text = page.extract_text() or ""

        # Détecter le début de l'Annexe A
        if re.search(r"Annexe\s*A", text, re.IGNORECASE):
            in_annexe_a = True

        # Détecter la fin (Annexe B ou suivante)
        if in_annexe_a and re.search(r"Annexe\s*B", text, re.IGNORECASE):
            break

        if in_annexe_a:
            tables = page.extract_tables()
            for table in tables:
                if table and len(table) > 1:
                    # Première ligne = headers
                    headers = [str(h).strip() if h else f"col_{j}" 
                               for j, h in enumerate(table[0])]
                    rows = []
                    for row in table[1:]:
                        cleaned = [str(cell).strip() if cell else "" for cell in row]
                        rows.append(cleaned)
                    
                    df = pl.DataFrame(
                        {headers[j]: [row[j] for row in rows] 
                         for j in range(len(headers))}
                    )
                    tables_found.append(df)
                    print(f"Page {i+1} : tableau extrait ({len(rows)} lignes, {len(headers)} colonnes)")

if tables_found:
    # Combiner tous les tableaux si même structure
    try:
        combined = pl.concat(tables_found, how="diagonal")
        print(f"\nTotal : {combined.shape[0]} lignes, {combined.shape[1]} colonnes")
        print(combined.head())
        combined.write_excel(OUTPUT_PATH)
        print(f"\nFichier sauvegardé : {OUTPUT_PATH}")
    except Exception as e:
        print(f"Tableaux de structures différentes, export séparé : {e}")
        for k, df in enumerate(tables_found):
            path = f"RPG_annexe_A_table_{k+1}.xlsx"
            df.write_excel(path)
            print(f"  → {path}")
else:
    print("Aucun tableau trouvé dans l'Annexe A.")