// tools/split_opening_and_movements.ts
import * as XLSX from "xlsx";
import fs from "fs";
import path from "path";

function toNumber(v:any){ if(v==null) return 0; const s=String(v).replace(/\./g,'').replace(/,/g,''); const n=Number(s); return isNaN(n)?0:n; }

function findValue(r:any, keys:string[]){
  const ks=Object.keys(r||{});
  for(const k of ks){
    const lk=k.toLowerCase();
    for(const c of keys){
      if(lk.includes(c)) return r[k];
    }
  }
  return undefined;
}

async function run(){
  const inFile = process.argv[2];
  if(!inFile){ console.error("usage: node split_opening_and_movements.js input.xlsx"); process.exit(1); }
  const wb = XLSX.readFile(inFile);
  const sheet = wb.SheetNames[0];
  const rows = XLSX.utils.sheet_to_json(wb.Sheets[sheet], {defval: ""}) as any[];

  const opening: any[] = [];
  const movements: any[] = [];

  for(const r of rows){
    const sku = (findValue(r, ["sku","ma","code","skud"])||"").toString().trim();
    const name = (findValue(r, ["name","ten","ten_goc"])||"").toString().trim();
    const ton_dau = toNumber(findValue(r, ["ton_dau","ton","tondau"]));
    const nhap = toNumber(findValue(r, ["nhap","in"]));
    const xuat = toNumber(findValue(r, ["xuat","out"]));
    const gia_goc = toNumber(findValue(r, ["gia_goc","gia"]));
    const location = (findValue(r, ["location","kho","warehouse"])||"").toString().trim();

    opening.push({ sku, name, location_code: location, opening_qty: ton_dau, opening_cost_per_unit: gia_goc });

    if(nhap>0) movements.push({ sku, name, movement_type: "IN", qty: nhap, unit_cost: gia_goc, date: new Date().toISOString(), location_code: location, note: "imported_from_mixed" });
    if(xuat>0) movements.push({ sku, name, movement_type: "OUT", qty: xuat, unit_cost: null, date: new Date().toISOString(), location_code: location, note: "imported_from_mixed" });
  }

  const wb1 = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb1, XLSX.utils.json_to_sheet(opening), "opening");
  const out1 = path.join(process.cwd(), "out_opening.xlsx");
  XLSX.writeFile(wb1, out1);

  const wb2 = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb2, XLSX.utils.json_to_sheet(movements), "movements");
  const out2 = path.join(process.cwd(), "out_movements.xlsx");
  XLSX.writeFile(wb2, out2);

  console.log("Written:", out1, out2);
}

run();
