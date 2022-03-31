# census_query:
# A set of functions for querying Census data
# By Kenneth Burchfiel
# Released under the MIT License

import pandas as pd


def generate_variable_and_group_lists(year, source, variable_filter):
    '''
    This function retrieves all variables listed on the Census data page 
    for a given year and dataset. (See below for options for the 'source'
    variable.)
    
    variable_filter refers to a string that is present in only 
    data variables. This helps clean up tables that contain rows that lack
    variable data. For instance, you can try using 'Estimate' as the 
    variable_filter value for American Community Survey data. 
    If you choose not to filter results or can't find a value
    that retrieves a list of only variable rows, you can enter '' as
    the value for variable_filter. This will retrieve a table containing
    all rows, but it should still be useful.

    '''

    if source == 'acs5': # American Community Survey (5-year estimates)
        source_string = '/acs/acs5'

    if source == 'acs1': # American Community Survey (5-year estimates)
        source_string = '/acs/acs1'

    if source == 'census_redistricting': # US Decennial Census redistricting
        # data
        source_string = '/dec/pl'

    if source == 'census_sf1': # US Decennial Census summary file data. 
        source_string = '/dec/sf1'
    
    # Note: At the time of writing this function, 2020 US Census data was not
    # yet fully available. Once it becomes available, I plan to add a new
    # 'source' option that covers that data, as I don't believe it will exist
    # under the /sf1 folder anymore.

    request_string = 'https://api.census.gov/data/'+str(year)+ \
        source_string+'/variables.html'
        # This variable stores the URL from which variable data will be 
        # retrieved.
    print("retrieving data from:",request_string)

    df_variables = pd.read_html(request_string)[0]
    # read_html returns a list of DataFrames, but only the first one is needed
    # (hence the inclusion of 0 [0])
    df_variables = df_variables.loc[df_variables['Label'].str.contains(
        variable_filter)].copy() 
        # Filters the DataFrame to exclude rows that do not contain variable
        # values. If variable_filter is '', no filtering will occur.
    df_variables = df_variables[['Name', 'Label', 'Concept', 'Group']] # Removes
    # extraneous columns
    df_variables.rename(columns={'Name':'Variable'},inplace=True) # Changing the 
    # name will make it easier to merge this table with ones
    # that also have a 'Name' column}
    df_variables.reset_index(drop=True,inplace=True)


    # The function next adds in a 'Description' column that combines the
    # 'Concept' and 'Label' data to describe what each variable code refers to.

    df_variables['Description'] = df_variables['Concept'] +\
        ' ' + df_variables['Label']
    df_variables.sort_values('Variable',inplace=True)
    df_variables.reset_index(drop=True,inplace=True)
    df_variables

    # The following line creates a shorter DataFrame with only group (table)
    # information to make locating groups easier.
    df_groups = df_variables[['Concept', 'Group']].drop_duplicates(
    ).reset_index(drop=True)

    return(df_variables, df_groups) # Items are returned in tuple form.



def retrieve_census_data(df_variable_list, year, source, region, api_key):
    ''' This function retrieves data from the US Census Bureau in batches
    of 45 variables at a time. (See below for options for the 'source'
    variable.)

    df_variable_list: a DataFrame containing information on variables
    and their descriptions. The generate_variable_and_group_lists function
    returns a variable table that can then be filtered by the user to create 
    df_variable_list.

    region: Can be 'zip', 'county', or 'state'. The function will then
    retrieve data for that region.

    year: the year for which to retrieve Census data.

    api_key: a US Census API key. You can download one for free at
    https://api.census.gov/data/key_signup.html .

    Note: the Examples pages on the Census website (such as
    https://api.census.gov/data/2019/acs/acs5/examples.html for acs5 data
    and https://api.census.gov/data/2010/dec/sf1/examples.html for decennial
    census data)
    were very helpful for creating this function.

    '''

    extra_columns = {} # Will store extra columns that will get added back
    # after variable data is retrieved

    if source == 'acs5': # American Community Survey (5-year estimates)
        source_string = '/acs/acs5'

    if source == 'acs1': # American Community Survey (5-year estimates)
        source_string = '/acs/acs1'

    if source == 'census_redistricting': # US Decennial Census redistricting
        # data
        source_string = '/dec/pl'

    if source == 'census_sf1': # US Decennial Census summary file data. 
        source_string = '/dec/sf1'
    
    # Note: At the time of writing this function, 2020 US Census data was not
    # yet fully available. Once it becomes available, I plan to add a new
    # 'source' option that covers that data, as I don't believe it will exist
    # under the /sf1 folder anymore.

    if region == 'zip':
        region_string = '&for=zip%20code%20tabulation%20area:*'

    if region == 'state':
        region_string = '&for=state:*'

    if region == 'county':
        region_string = '&for=county:*&in=state:*'

    # The following for loop retrieves data from the Census website in groups
    # of 45 variables at a time.
    # First, the function creates a string (variable_string) containing codes 
    # for up to 45 variables.
    for start_point in range(0, len(df_variable_list), 45):
        variable_string = '' # This string will contain all the variables to be 
        # retrieved from the Census API.
        end_point = min(start_point + 45, len(df_variable_list)) # variables 
        # will stop being added to the list once (A) the end of the DataFrame 
        # is reached or (B) variable_string contains 45 variables.
        for i in range(start_point, end_point):
            if i != end_point-1:
                variable_string += df_variable_list.iloc[i]['Variable'] + ','
            else:
                variable_string += df_variable_list.iloc[i]['Variable']
        print("Retrieving data from rows",start_point,"to",end_point-1) # The 
        # last row is not included due to how slice notation works

        # Next, the actual URL for the API call will be initialized.

        request_string = 'https://api.census.gov/data/'+str(year)+\
            source_string+'?get=NAME,'+variable_string+\
                region_string+'&key='+api_key
    

        # The data retrieved by this URL then gets stored into a DataFrame
        # (batch_request).
        batch_request = pd.read_json(request_string)

        # The following two lines convert the first row of batch_request
        # into the header, then drop that row from batch_request.
        batch_request.columns = batch_request.iloc[0] 
        batch_request = batch_request.iloc[1:]

        # Depending on the region specified, there will also be 'state', 
        # 'zip code tabulation area', or 'county' columns in the batch_request
        # DataFrame. To simplify the table merging process, each extra
        # column will be added to its own DataFrame and then dropped from
        # batch_request. The extra columns will be merged back into the
        # DataFrame containing all variables near the end of the function.

        if 'state' in batch_request.columns:
            if start_point == 0:
                extra_columns['state'] = pd.DataFrame(
                    batch_request[['NAME', 'state']])
            batch_request.drop('state', axis = 1, inplace = True)
            

        if 'county' in batch_request.columns:
            if start_point == 0:
                extra_columns['county'] = pd.DataFrame(
                    batch_request[['NAME', 'county']])
            batch_request.drop('county', axis = 1, inplace = True)

        # The zip code column does not contain any information not already
        # present in the 'NAME' column, so it can be dropped.
        if 'zip code tabulation area' in batch_request.columns:
            batch_request.drop(
                'zip code tabulation area', axis = 1, inplace = True)
        
        # Data from each iteration of the for loop will be stored in
        # a DataFrame called df_region_data. The following code block
        # either initializes df_region_data as batch_request (if df_county_data 
        # does not yet exist) or merges the new copy of batch_request 
        # into df_region_data.

        if start_point == 0:
            df_region_data = batch_request 
        else:
            df_region_data = df_region_data.merge(
                batch_request, on = 'NAME', how = 'outer')


    # Now that all variable data has been obtained from the census, changes can 
    # be applied to the resulting dataframe as a whole.

    # Currently, the column names are mostly ID-based (e.g. 'B01001_001E',
    # 'B01002_001E', 'B06008_001E', 'B06008_003E'). To make them more intuitive
    # (at the expense of dramatically increasing their length), they will be
    #  replaced with values within df_variable_list's 'Description' column.

    # Since the only columns in the DataFrame are the 'NAME' column
    # and these variable ID columns, this is a good opportunity
    # to rename the variable ID columns (as there are no other columns
    # to deal with).
    descriptions = ['NAME'] # This will remain as the first column
    descriptions.extend(df_variable_list['Description'])
    df_region_data.columns = descriptions


    df_region_data.insert(1, 'Year', year) # Stores the year of the Census data 
    # as the first column in the DataFrame

    # Extra state/county/information columns that existed in earlier versions 
    # of the dataset can now be merged back into the DataFrame.

    if 'county' in extra_columns.keys():
        df_region_data = df_region_data.merge(extra_columns['county'], 
        on = 'NAME', how = 'outer')
        df_region_data.insert(2, 'county', df_region_data.pop('county'))

    if 'state' in extra_columns.keys():
        df_region_data = df_region_data.merge(extra_columns['state'], 
        on = 'NAME', how = 'outer')
        df_region_data.insert(2, 'state', df_region_data.pop('state'))


    # If 'zip' was selected as the region, the 'NAME' column of df_region_data
    # contains 'ZCTA ' before each zip number. The following function removes
    # this value, and then fills in any missing zip code numbers with zeroes.
    # (Some zip codes begin with zeroes, and if these are converted to 
    # numerical data, the leading zeroes will be removed. This shouldn't 
    # occur in this case, but just in case, zfill is called to add those
    # leading zeroes back in.)
    if region == 'zip':
        df_region_data['NAME'] = df_region_data['NAME'].str.replace(
            'ZCTA5 ','').str.zfill(5) 

    # The following for loop converts all numerical results
    # columns (e.g. all but the first column, which stores region name
    # information into numerical values.

    for i in range(1, len(df_region_data.columns)):
        df_region_data.iloc[:,i] = pd.to_numeric(df_region_data.iloc[:,i])
    
    return df_region_data


def retrieve_single_census_variable(region, year, source, column_name, 
variable, api_key):

    ''' This function is similar to retrieve_census_data except that it only
    obtains data for a single variable. (See retrieve_census_data for 
    additional documentation.)

    variable refers to a specific variable code (e.g. 'B01001_001E' for the
    American Community Survey or 'P001001' for the US Census).

    column_name refers to the desired name for the variable column. Whereas
    retrieve_census_data initialized column names as descriptions from
    df_variable_list, that DataFrame is not passed to this function, so the
    user must provide a string. (The 'year' value will also be added to this
    column name in case it differs from other years in a DataFrame to which
    the output of this function will be merged.)

    '''

    if source == 'acs5':
        source_string = '/acs/acs5'

    if source == 'acs1':
        source_string = '/acs/acs1'

    if source == 'census_redistricting':
        source_string = '/dec/pl'

    if source == 'census_sf1':
        source_string = '/dec/sf1'

    if region == 'zip':
        request_string = 'https://api.census.gov/data/'+str(year)+\
            source_string+'?get=NAME,'+variable+\
                '&for=zip%20code%20tabulation%20area:*&key='+api_key

    if region == 'state':
        request_string = 'https://api.census.gov/data/'+str(year)+\
            source_string+'?get=NAME,'+variable+'&for=state:*&key='+api_key

    if region == 'county':
        request_string = 'https://api.census.gov/data/'+str(year)+\
            source_string+'?get=NAME,'+variable+\
                '&for=county:*&in=state:*&key='+api_key
    
    df_result = pd.read_json(request_string)

    df_result.columns = df_result.iloc[0] 
    df_result = df_result.iloc[1:]
    # The column_name and year values will be used to create the name of
    # the column containing variable data.
    result_col = str(column_name)+'_'+str(year)
    df_result.rename(columns={variable:result_col},inplace=True)
    df_result[result_col] = pd.to_numeric(df_result[result_col])

    if region == 'zip':
        df_result['NAME'] = df_result['NAME'].str.replace(
            'ZCTA5 ','').str.zfill(5)


    return(df_result[['NAME', result_col]])


def test_variables(df_variable_list, year, source, region, api_key):
    ''' This function is designed to determine which variable codes are causing
    the batch request process in retrieve_census_data to return an error.
    See retrieve_census_data for additional documentation.
    '''

    if source == 'acs5':
        source_string = '/acs/acs5'

    if source == 'acs1':
        source_string = '/acs/acs1'

    if source == 'census_redistricting':
        source_string = '/dec/pl'

    if source == 'census_sf1':
        source_string = '/dec/sf1'

    if region == 'zip':
        region_string = '&for=zip%20code%20tabulation%20area:*'

    if region == 'state':
        region_string = '&for=state:*'

    if region == 'county':
        region_string = '&for=county:*&in=state:*'

    for i in range(len(df_variable_list)):

    
        request_string = 'https://api.census.gov/data/'+str(year)+\
            source_string+'?get=NAME,'+df_variable_list[i]+\
                region_string+'&key='+api_key

        try:            
            pd.read_json(request_string)
            # print("Data for:",variable_list[i],"retrieved successfully.")
        except:
            print("Failed to retrieve data for:",df_variable_list[i]+". \
Confirm that the variable code was entered correctly and that this data \
is available for the specified region.")



def compare_variable_across_years(variable, variable_name, source, 
year_list, region, api_key):
    '''
    This function retrieves Census data on a single variable across multiple
    years, then merges that data into a DataFrame. It then calculates
    percentage changes (expressed in decimal format, e.g. 0.1 for a 10% change)
    from each year to the next, along with the total change from the first
    year provided to the last.

    variable refers to a specific variable code (e.g. 'B01001_001E' for the
    American Community Survey or 'P001001' for the US Census).

    variable_name refers to the desired name for the variable column (e.g.
    'population' for a column containing population data.)

    See retrieve_census_data for additional documentation.
    '''

    extra_columns = {}

    if source == 'acs5':
        source_string = '/acs/acs5'

    if source == 'acs1':
        source_string = '/acs/acs1'

    if source == 'census_redistricting':
        source_string = '/dec/pl'

    if source == 'census_sf1':
        source_string = '/dec/sf1'

    if region == 'zip':
        region_string = '&for=zip%20code%20tabulation%20area:*'

    if region == 'state':
        region_string = '&for=state:*'

    if region == 'county':
        region_string = '&for=county:*&in=state:*'
    

    for i in range(len(year_list)):
        year = year_list[i]
        print("Retrieving data for:",year)

        request_string = 'https://api.census.gov/data/'+str(year
        )+source_string+'?get=NAME,'+variable+region_string+'&key='+api_key
            
        df_year = pd.read_json(request_string)

        df_year.columns = df_year.iloc[0] 
        df_year = df_year.iloc[1:]
        # The following line renames each variable column based on
        # variable_name and the current year in the for loop.

        df_year.rename(columns={variable:str(
            variable_name)+'_'+str(year)},inplace=True)
        if 'state' in df_year.columns:
            if i == 0:
                extra_columns['state'] = pd.DataFrame(
                    df_year[['NAME', 'state']])
            df_year.drop('state', axis = 1, inplace = True)

        if 'zip code tabulation area' in df_year.columns:
            df_year.drop('zip code tabulation area', axis = 1, inplace = True)

        if 'county' in df_year.columns:
            if i == 0:
                extra_columns['county'] = pd.DataFrame(
                    df_year[['NAME', 'county']])
            df_year.drop('county', axis = 1, inplace = True)

        if i == 0:
            df_data_across_years = df_year 
        else:
            df_data_across_years = df_data_across_years.merge(
                df_year, on = 'NAME', how = 'outer')

    # In order to calculate percentage changes, each data column must be
    # converted to numerical format. (The first column contains each region's
    # name and is thus skipped by the following for loop.)
    for i in range(1, len(df_data_across_years.columns)):
        df_data_across_years.iloc[:,i] = pd.to_numeric(
            df_data_across_years.iloc[:,i])


    # Next, percentage changes from each year to the next will be calculated,
    # along with the change from the first year of data to the last.

    data_column_count = len(df_data_across_years.columns)-1
    # Stores the number of data columns, which is currently equal
    # to the number of columns in the DataFrame minus one (the 
    # region name column).

    if data_column_count > 1: # If only one year was provided,
        # it won't be possible to compute percentage changes.


        # The following for loop starts at 2 so that it can skip the first
        # column (which contains only region name information) and access
        # the column containing the previous year's data (which will be 
        # necessary in order to calculate a percentage change).
        # It then creates a new column with percentage change information.
        # [-1:-5:-1][::-1] converts column names with both variable names
        # and years into just their years. 
        # Adding [-1:-5:-1] to a column name first retrieves the last
        # four characters of the column name (e.g. its year). However,
        # these characters are in reversed order, so [::-1] is added on
        # to re-reverse them.
        # These years are then used within each percentage change column
        # instead of the original variable column names in order to save space.
        for i in range(2, data_column_count+1):
            df_data_across_years[df_data_across_years.columns[
            i-1][-1:-5:-1][::-1]+'_to_'+df_data_across_years.columns[
            i][-1:-5:-1][::-1]+'_chg'] = (df_data_across_years[
            df_data_across_years.columns[i]] / df_data_across_years[
                df_data_across_years.columns[i-1]])-1

        # If there are 4 or more columns (and thus three or more years
        # being compared), then the following if statement
        # will also calculate the percentage change from the first 
        # data column to the last one.
        # data_column_count is used instead of 
        # len(df_data_across_years.columns) to determine the location of the
        # final data column because the addition of percentage change columns
        # has changed the total column count.
        # Note that, if data_column_count is 3, then
        # df_data_across_years[3] will access the 4th column in the DataFrame,
        # which is indeed the location of the final data column.
        if len(df_data_across_years.columns) > 3:
            df_data_across_years[df_data_across_years.columns[
            1][-1:-5:-1][::-1]+'_to_'+df_data_across_years.columns[
                data_column_count][-1:-5:-1][::-1]+'_chg'] = (
                    df_data_across_years[df_data_across_years.columns[
                data_column_count]] / df_data_across_years[
            df_data_across_years.columns[1]])-1

    if 'county' in extra_columns.keys():
        df_data_across_years = df_data_across_years.merge(
            extra_columns['county'], on = 'NAME', how = 'outer')
        df_data_across_years.insert(
            1, 'county', df_data_across_years.pop('county'))

    if 'state' in extra_columns.keys():
        df_data_across_years = df_data_across_years.merge(
            extra_columns['state'], on = 'NAME', how = 'outer')
        df_data_across_years.insert(
            1, 'state', df_data_across_years.pop('state'))

    if region == 'zip':
        df_data_across_years['NAME'] = \
            df_data_across_years['NAME'].str.replace('ZCTA5 ','').str.zfill(5) 

    return df_data_across_years
