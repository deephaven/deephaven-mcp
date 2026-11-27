// How much of the JS API a type-directed membrane would cover: classify every
// member of every class/interface in the given .d.ts files.
import ts from "typescript";

const files = process.argv.slice(2);
const program = ts.createProgram(files, { skipLibCheck: true });
const checker = program.getTypeChecker();

const counts: Record<string, number> = {};
const examples: Record<string, string[]> = {};
const bump = (k: string, ex: string) => {
  counts[k] = (counts[k] ?? 0) + 1;
  (examples[k] ??= []).length < 4 && examples[k].push(ex);
};
let types = 0;

const isFn = (t: ts.Type) => t.getCallSignatures().length > 0;
const isPromise = (t: ts.Type) => t.getSymbol()?.getName() === "Promise";

for (const sf of program.getSourceFiles()) {
  if (!files.some((f) => sf.fileName.endsWith(f.replace(/^.*node_modules\//, "")))) continue;
  const visit = (node: ts.Node, ns: string) => {
    if (ts.isModuleDeclaration(node) && node.body) {
      const name = node.name.getText(sf);
      ts.forEachChild(node.body, (c) => visit(c, ns ? `${ns}.${name}` : name));
      return;
    }
    if (ts.isClassDeclaration(node) || ts.isInterfaceDeclaration(node)) {
      types += 1;
      const tname = `${ns}.${node.name?.getText(sf)}`;
      for (const m of node.members) {
        const mname = m.name?.getText(sf) ?? "(index/call)";
        const where = `${tname}.${mname}`;
        if (ts.isConstructorDeclaration(m)) { bump("constructor (new)", tname); continue; }
        const isStatic = (ts.getCombinedModifierFlags(m as ts.Declaration) & ts.ModifierFlags.Static) !== 0;
        if (ts.isMethodDeclaration(m) || ts.isMethodSignature(m)) {
          const sig = checker.getSignatureFromDeclaration(m);
          if (!sig) continue;
          const ret = checker.getReturnTypeOfSignature(sig);
          const callbackParam = sig.getParameters().some((p) => isFn(checker.getTypeOfSymbolAtLocation(p, m)));
          if (callbackParam) bump("method taking a callback", where);
          if (isPromise(ret)) bump(`${isStatic ? "static " : ""}method -> Promise`, where);
          else if (isFn(ret)) bump("method -> function", where);
          else bump(`${isStatic ? "static " : ""}method -> sync value`, where);
          if (sig.typeParameters?.length) bump("generic method", where);
          continue;
        }
        if (ts.isPropertyDeclaration(m) || ts.isPropertySignature(m) || ts.isGetAccessor(m)) {
          const t = checker.getTypeAtLocation(m);
          if (isFn(t)) bump("function-valued property", where);
          else bump(`${isStatic ? "static " : ""}property / getter`, where);
          continue;
        }
        if (ts.isSetAccessor(m)) bump("setter", where);
        else if (ts.isIndexSignatureDeclaration(m)) bump("index signature", where);
      }
    }
    ts.forEachChild(node, (c) => visit(c, ns));
  };
  visit(sf, "");
}
console.log(`types: ${types}`);
for (const [k, v] of Object.entries(counts).sort((a, b) => b[1] - a[1])) {
  console.log(`${String(v).padStart(5)}  ${k}   e.g. ${examples[k]?.join(", ")}`);
}
