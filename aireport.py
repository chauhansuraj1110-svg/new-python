import os
import re
import io
from datetime import datetime
from PIL import Image
from flask import Flask, request, render_template, redirect, url_for, send_from_directory
import pdfplumber
import pytesseract
import pandas as pd
from werkzeug.utils import secure_filename

# -------- Config --------
UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"pdf", "png", "jpg", "jpeg", "tif", "tiff"}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs("cleaned_data", exist_ok=True)

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# -------- Reference ranges (example, adjust per lab) --------
# Each entry: (low, high, unit)
REFERENCE_RANGES = {
    "hemoglobin": (13.5, 17.5, "g/dL"),         # adult male example
    "rbc": (4.5, 5.9, "10^6/uL"),
    "wbc": (4.0, 11.0, "10^3/uL"),
    "platelets": (150, 450, "10^3/uL"),
    "glucose_fasting": (70, 99, "mg/dL"),
    "cholesterol": (0, 200, "mg/dL"),
    "hdl": (40, 60, "mg/dL"),
    "ldl": (0, 100, "mg/dL"),
    "triglycerides": (0, 150, "mg/dL"),
    "alt": (7, 56, "U/L"),
    "ast": (10, 40, "U/L"),
    "creatinine": (0.7, 1.3, "mg/dL"),
    "urea": (7, 20, "mg/dL"),
    "sodium": (135, 145, "mmol/L"),
    "potassium": (3.5, 5.0, "mmol/L"),
}

# synonyms / patterns to search for each marker
MARKER_PATTERNS = {
    "hemoglobin": [r"hemoglobin[:\s]*([0-9]+\.?[0-9]*)", r"\bHB[:\s]*([0-9]+\.?[0-9]*)", r"\bHgb[:\s]*([0-9]+\.?[0-9]*)"],
    "wbc": [r"\bWBC[:\s]*([0-9]+\.?[0-9]*)", r"white blood cell[:\s]*([0-9]+\.?[0-9]*)"],
    "rbc": [r"\bRBC[:\s]*([0-9]+\.?[0-9]*)"],
    "platelets": [r"\bPlatelet[s]*[:\s]*([0-9]+\.?[0-9]*)"],
    "glucose_fasting": [r"glucose fasting[:\s]*([0-9]+\.?[0-9]*)", r"fbg[:\s]*([0-9]+\.?[0-9]*)"],
    "cholesterol": [r"cholesterol[:\s]*([0-9]+\.?[0-9]*)", r"total cholesterol[:\s]*([0-9]+\.?[0-9]*)"],
    "hdl": [r"\bHDL[:\s]*([0-9]+\.?[0-9]*)"],
    "ldl": [r"\bLDL[:\s]*([0-9]+\.?[0-9]*)"],
    "triglycerides": [r"\bTriglyceride[s]*[:\s]*([0-9]+\.?[0-9]*)", r"\bTG[:\s]*([0-9]+\.?[0-9]*)"],
    "alt": [r"\bALT[:\s]*([0-9]+\.?[0-9]*)", r"alanine transaminase[:\s]*([0-9]+\.?[0-9]*)"],
    "ast": [r"\bAST[:\s]*([0-9]+\.?[0-9]*)", r"aspartate transaminase[:\s]*([0-9]+\.?[0-9]*)"],
    "creatinine": [r"\bCreatinine[:\s]*([0-9]+\.?[0-9]*)"],
    "urea": [r"\bUrea[:\s]*([0-9]+\.?[0-9]*)", r"\bBUN[:\s]*([0-9]+\.?[0-9]*)"],
    "sodium": [r"\bSodium[:\s]*([0-9]+\.?[0-9]*)"],
    "potassium": [r"\bPotassium[:\s]*([0-9]+\.?[0-9]*)"],
}

# -------- Helpers --------
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def extract_text_from_pdf(path):
    text = []
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text.append(page_text)
    except Exception as e:
        print("pdf extraction error:", e)
    return "\n".join(text)

def extract_text_from_image(path):
    img = Image.open(path)
    text = pytesseract.image_to_string(img)
    return text

def extract_text(path):
    ext = path.rsplit(".", 1)[1].lower()
    if ext == "pdf":
        return extract_text_from_pdf(path)
    else:
        return extract_text_from_image(path)

def find_marker_values(text):
    text_lower = text.lower()
    results = {}
    for marker, patterns in MARKER_PATTERNS.items():
        found = None
        for pat in patterns:
            m = re.search(pat, text_lower, flags=re.IGNORECASE)
            if m:
                try:
                    val = float(m.group(1))
                    found = val
                    break
                except:
                    continue
        results[marker] = found
    return results

def analyze_results(values_dict):
    # compare with reference ranges and create status/suggestions
    summary = []
    within = 0
    total = 0
    for marker, val in values_dict.items():
        low, high, unit = REFERENCE_RANGES.get(marker, (None, None, ""))
        entry = {"marker": marker, "value": val, "unit": unit, "status": "unknown", "low": low, "high": high, "advice": ""}
        if val is None:
            entry["status"] = "missing"
            entry["advice"] = "Value not found in report."
        else:
            total += 1
            if low is not None and high is not None:
                if low <= val <= high:
                    entry["status"] = "normal"
                    entry["advice"] = "Within reference range."
                    within += 1
                elif val < low:
                    entry["status"] = "low"
                    entry["advice"] = "Below reference range — consider clinical correlation."
                else:
                    entry["status"] = "high"
                    entry["advice"] = "Above reference range — consider clinical correlation."
            else:
                entry["status"] = "unknown"
        summary.append(entry)
    # Health score: fraction of found markers that are normal (0-100)
    score = int((within / total) * 100) if total > 0 else 0
    return summary, score, total

def pretty_name(key):
    return key.replace("_", " ").title()

# -------- Routes --------
@app.route("/", methods=["GET"])
def index():
    return render_template("upload.html")

@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return {"error": "No file part"}, 400
    file = request.files["file"]
    if file.filename == "":
        return {"error": "No selected file"}, 400
    if file and allowed_file(file.filename):
        filename = secure_filename(f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{file.filename}")
        path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(path)

        # extract text
        extracted = extract_text(path)
        # find markers
        values = find_marker_values(extracted)
        summary, score, total = analyze_results(values)

        # save raw extracted text for debug
        with open(path + ".txt", "w", encoding="utf-8") as f:
            f.write(extracted)

        # Save cleaned CSV locally (no DB changes)
        df = pd.DataFrame(summary)
        csv_name = os.path.join("cleaned_data", f"{filename}.csv")
        df.to_csv(csv_name, index=False)

        return render_template("report.html", summary=summary, score=score, total=total, pretty_name=pretty_name, filename=filename)
    else:
        return {"error": "File type not allowed"}, 400

@app.route("/uploads/<path:filename>")
def uploaded_file(filename):
    return send_from_directory(app.config["UPLOAD_FOLDER"], filename)

if __name__ == "__main__":
    app.run(debug=True)
