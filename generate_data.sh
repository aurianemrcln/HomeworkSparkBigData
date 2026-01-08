#!/bin/bash

# Create a base data directory (using Linux style path for Git Bash)
# /c/temp/data maps to C:\temp\data on Windows
BASE_DIR="data_generated"
mkdir -p "$BASE_DIR"

echo "Generating dummy data in $BASE_DIR..."

# Create specific folder for that day
mkdir -p "$BASE_DIR"

# Loop 50 days
for n in $(seq 0 10); do
    # Calculate date
    day=$(date -d "2025-01-01 +${n} day" +%Y-%m-%d)
    
    
    
    # Create a CSV file
    # We make a small change on Day 5 to test 'Updates'
    if [ $n -eq 5 ]; then
        # Day 25: Same ID (1), but address changes to 'Avenue des Champs'
        echo "uid_adresse;cle_interop;commune_insee;commune_nom;commune_deleguee_insee;commune_deleguee_nom;voie_nom;lieudit_complement_nom;numero;suffixe;position;x;y;long;lat;cad_parcelles;source;date_der_maj;certification_commune" > "$BASE_DIR/dump-${day}.csv"
        echo " @a:42de9134-7774-4297-a0f5-bb00fac139a2 @v:dbaebc5f-77dc-458c-95c1-462591a05252 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_g0ru02_00108;01001;L'Abergement-Clémenciat;;;Clemencia;;108;;parcelle;848494.87;6561148.7;4.9235822625;46.133867162499996;01001000ZL0263;cadastre;;0
 @a:3a03f26d-9e34-4d96-a7bf-7594046006e0 @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00009;01001;L'Abergement-Clémenciat;;;Imp des Epis;;9;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:0ff28630-f204-4191-9f64-ccde2dda8bd2 @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00040;01001;L'Abergement-Clémenciat;;;Imp des Epis;;40;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:bc8c5eb0-f22c-494d-b71c-3af82dfef37b @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00048;01001;L'Abergement-Clémenciat;;;Imp des Epis;;48;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:48ddf9cb-e370-451a-a5e9-7f7b2479e4dd @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00049;01001;L'Abergement-Clémenciat;;;Imp des Epis;;49;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:735132a7-bbf6-4461-b960-a65c0b0bf331 @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00057;01001;L'Abergement-Clémenciat;;;Imp des Epis;;57;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:3926c795-582f-45a3-ba96-bd6cef16b09a @v:f7364620-9c13-42a9-bef5-a9a17a0f9b88 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_0165_00019;01001;L'Abergement-Clémenciat;;;Route de la Fontaine;;19;;entrée;848193.38;6563109.52;4.920296;46.151585;;inconnue;;0
 @a:df4e1455-cbef-49cd-8276-477820432d3a @v:f7364620-9c13-42a9-bef5-a9a17a0f9b88 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_0165_00048;01001;L'Abergement-Clémenciat;;;Route de la Fontaine;;48;;segment;848184.02;6563066.08;4.920161;46.151196;;inconnue;;0
" >> "$BASE_DIR/dump-${day}.csv"
    else
        # Normal Days: Original address
        echo "uid_adresse;cle_interop;commune_insee;commune_nom;commune_deleguee_insee;commune_deleguee_nom;voie_nom;lieudit_complement_nom;numero;suffixe;position;x;y;long;lat;cad_parcelles;source;date_der_maj;certification_commune" > "$BASE_DIR/dump-${day}.csv"
        echo " @a:4202e761-3fed-4a6c-86dd-49c4a81070ac @v:8d81b68b-f389-4857-aff0-2ce380aa44f0 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_4b50r5_00630;01001;L'Abergement-Clémenciat;;;la Chèvre;;630;;segment;847780.79;6560977.36;4.914282;46.132481;;inconnue;;0
 @a:42de9134-7774-4297-a0f5-bb00fac139a2 @v:dbaebc5f-77dc-458c-95c1-462591a05252 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_g0ru02_00108;01001;L'Abergement-Clémenciat;;;Clemencia;;108;;parcelle;848494.87;6561148.7;4.9235822625;46.133867162499996;01001000ZL0263;cadastre;;0
 @a:3a03f26d-9e34-4d96-a7bf-7594046006e0 @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00009;01001;L'Abergement-Clémenciat;;;Imp des Epis;;9;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:6b32bd47-16a0-494f-a3c2-ca56f90a5a31 @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00023;01001;L'Abergement-Clémenciat;;;Imp des Epis;;23;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:26c1dac2-6573-4b38-9f49-de1603dd5680 @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00026;01001;L'Abergement-Clémenciat;;;Imp des Epis;;26;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:eb3e0d3b-a3f6-465d-a0fd-759921226569 @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00028;01001;L'Abergement-Clémenciat;;;Imp des Epis;;28;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:be28756c-225b-463e-967e-b61106e935d1 @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00033;01001;L'Abergement-Clémenciat;;;Imp des Epis;;33;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:ed47c237-e4d6-44fe-b9f2-e3594060bb99 @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00035;01001;L'Abergement-Clémenciat;;;Imp des Epis;;35;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:0ff28630-f204-4191-9f64-ccde2dda8bd2 @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00040;01001;L'Abergement-Clémenciat;;;Imp des Epis;;40;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:bc8c5eb0-f22c-494d-b71c-3af82dfef37b @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00048;01001;L'Abergement-Clémenciat;;;Imp des Epis;;48;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:48ddf9cb-e370-451a-a5e9-7f7b2479e4dd @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00049;01001;L'Abergement-Clémenciat;;;Imp des Epis;;49;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:735132a7-bbf6-4461-b960-a65c0b0bf331 @v:712d2b77-a5dc-4e31-9766-664ddfd8f2f2 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_ngzlqw_00057;01001;L'Abergement-Clémenciat;;;Imp des Epis;;57;;;848670.66;6563239.01;4.926519;46.152646;;arcep;;0
 @a:3926c795-582f-45a3-ba96-bd6cef16b09a @v:f7364620-9c13-42a9-bef5-a9a17a0f9b88 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_0165_00019;01001;L'Abergement-Clémenciat;;;Route de la Fontaine;;19;;entrée;848193.38;6563109.52;4.920296;46.151585;;inconnue;;0
 @a:5e191029-c3dc-42ee-8387-d7fbc78daf85 @v:f7364620-9c13-42a9-bef5-a9a17a0f9b88 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_0165_00035;01001;L'Abergement-Clémenciat;;;Route de la Fontaine;;35;;entrée;848203.98;6563089.67;4.920427;46.151404;;inconnue;;0
 @a:df4e1455-cbef-49cd-8276-477820432d3a @v:f7364620-9c13-42a9-bef5-a9a17a0f9b88 @c:cb7c35b3-6e11-4d2c-b590-84984daed77e;01001_0165_00048;01001;L'Abergement-Clémenciat;;;Route de la Fontaine;;48;;segment;848184.02;6563066.08;4.920161;46.151196;;inconnue;;0
" >> "$BASE_DIR/dump-${day}.csv"
    fi
done

echo "Data generation complete."