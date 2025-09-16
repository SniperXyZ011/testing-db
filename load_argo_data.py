import netCDF4
import psycopg2
import numpy as np
from datetime import datetime, timedelta

def load_argo_nc_to_postgres(nc_file_path, db_params):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        print("✅ Successfully connected to the database.")
    except psycopg2.OperationalError as e:
        print(f"❌ Could not connect to the database: {e}")
        return

    print(f"🔄 Reading from NetCDF file: {nc_file_path}")
    with netCDF4.Dataset(nc_file_path, 'r') as nc_file:
        platform_number = int(nc_file.variables['PLATFORM_NUMBER'][0].tobytes().decode('utf-8').strip())
        
        print(f"  - Found Platform Number: {platform_number}")
        
        insert_float_sql = """
        INSERT INTO floats (platform_number, project_name, pi_name, platform_type, float_serial_no, wmo_inst_type)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (platform_number) DO NOTHING;
        """
        project_name = nc_file.variables['PROJECT_NAME'][0].tobytes().decode('utf-8').strip()
        pi_name = nc_file.variables['PI_NAME'][0].tobytes().decode('utf-8').strip()
        platform_type = nc_file.variables['PLATFORM_TYPE'][0].tobytes().decode('utf-8').strip()
        float_serial_no = nc_file.variables['FLOAT_SERIAL_NO'][0].tobytes().decode('utf-8').strip()
        wmo_inst_type = nc_file.variables['WMO_INST_TYPE'][0].tobytes().decode('utf-8').strip()

        cur.execute(insert_float_sql, (platform_number, project_name, pi_name, platform_type, float_serial_no, wmo_inst_type))
        print(f"  - Upserted float {platform_number} into 'floats' table.")

        num_profiles = len(nc_file.dimensions['N_PROF'])
        print(f"  - Found {num_profiles} profiles in this file.")
        
        reference_date_time = datetime.strptime(
            nc_file.variables['REFERENCE_DATE_TIME'][:].tobytes().decode('utf-8').strip(), 
            '%Y%m%d%H%M%S'
        )

        for i in range(num_profiles):
            cycle_number = int(nc_file.variables['CYCLE_NUMBER'][i])
            print(f"\n  - Processing Profile {i+1}/{num_profiles} (Cycle: {cycle_number})")
            
            direction = nc_file.variables['DIRECTION'][i].tobytes().decode('utf-8').strip()
            juld = nc_file.variables['JULD'][i]
            profile_time = None
            if not np.ma.is_masked(juld):
                profile_time = reference_date_time + timedelta(days=float(juld))
                
            latitude = nc_file.variables['LATITUDE'][i]
            longitude = nc_file.variables['LONGITUDE'][i]
            
            location_wkt = None
            if not np.ma.is_masked(latitude) and not np.ma.is_masked(longitude):
                location_wkt = f"POINT({longitude} {latitude})"
            
            profile_pres_qc = nc_file.variables['PROFILE_PRES_QC'][i].tobytes().decode('utf-8').strip()
            profile_temp_qc = nc_file.variables['PROFILE_TEMP_QC'][i].tobytes().decode('utf-8').strip()
            profile_psal_qc = nc_file.variables['PROFILE_PSAL_QC'][i].tobytes().decode('utf-8').strip()
            
            insert_profile_sql = """
            INSERT INTO profiles (platform_number, cycle_number, direction, profile_time, location, profile_pres_qc, profile_temp_qc, profile_psal_qc)
            VALUES (%s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326), %s, %s, %s)
            ON CONFLICT (platform_number, cycle_number) DO NOTHING;
            """
            cur.execute(insert_profile_sql, (platform_number, cycle_number, direction, profile_time, location_wkt, profile_pres_qc, profile_temp_qc, profile_psal_qc))
            
            measurements_count = 0
            num_levels = len(nc_file.dimensions['N_LEVELS'])
            pres_adjusted = nc_file.variables['PRES_ADJUSTED'][i, :]
            temp_adjusted = nc_file.variables['TEMP_ADJUSTED'][i, :]
            psal_adjusted = nc_file.variables['PSAL_ADJUSTED'][i, :]
            pres_adjusted_qc = nc_file.variables['PRES_ADJUSTED_QC'][i, :].tobytes().decode('utf-8')
            temp_adjusted_qc = nc_file.variables['TEMP_ADJUSTED_QC'][i, :].tobytes().decode('utf-8')
            psal_adjusted_qc = nc_file.variables['PSAL_ADJUSTED_QC'][i, :].tobytes().decode('utf-8')

            for j in range(num_levels):
                if not np.ma.is_masked(pres_adjusted[j]):
                    insert_measurement_sql = """
                    INSERT INTO measurements (platform_number, cycle_number, pres_adjusted, pres_adjusted_qc, temp_adjusted, temp_adjusted_qc, psal_adjusted, psal_adjusted_qc)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (platform_number, cycle_number, pres_adjusted) DO NOTHING;
                    """
                    temp_val = float(temp_adjusted[j]) if not np.ma.is_masked(temp_adjusted[j]) else None
                    psal_val = float(psal_adjusted[j]) if not np.ma.is_masked(psal_adjusted[j]) else None
                    
                    cur.execute(insert_measurement_sql, (
                        platform_number, cycle_number, float(pres_adjusted[j]),
                        pres_adjusted_qc[j], temp_val, temp_adjusted_qc[j],
                        psal_val, psal_adjusted_qc[j]
                    ))
                    measurements_count += 1
            print(f"    -> Inserted 1 profile row and {measurements_count} measurement rows.")


    print("\nCommitting transaction to database...")
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Data from {nc_file_path} loaded successfully.")

if __name__ == '__main__':
    db_connection_params = {
        "host": "localhost",
        "database": "argodb",
        "user": "argo",
        "password": "mysecretpassword"
    }
    netcdf_file = "20250912_prof.nc"
    load_argo_nc_to_postgres(netcdf_file, db_connection_params)