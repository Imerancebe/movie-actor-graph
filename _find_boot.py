import zipfile
import glob

for jar in glob.glob(r"d:\bigmaths\_neo4j_extract\var\lib\neo4j\lib\*.jar"):
    try:
        with zipfile.ZipFile(jar) as z:
            for name in z.namelist():
                if "Neo4jBoot" in name or ("server" in name and name.endswith("EntryPoint.class")):
                    print(jar.split("\\")[-1], "->", name)
    except zipfile.BadZipFile:
        pass
