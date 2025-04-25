
class DMIJSONUtils:
    
    def get_bbox(json_str):
        """
        Takes json string from a DMI climate grid file
        Returns bbox coordinates
        """
        return json_str['geometry']['coordinates']
    
    
    def get_value(json_str):
        """
        Takes json string from a DMI climate grid file
        Returns the value associated with the parameter
        """
        return json_str['properties']['value']
    
    
    def get_from_time(json_str):
        """
        Takes json string from a DMI climate grid file
        Returns the timestamp the measurement started
        """
        return json_str['properties']['from']
    
    
    def get_parameter_id(json_str):
        """
        Takes json string from a DMI climate grid file
        Returns the parameterId
        """
        return json_str['properties']['parameterId']
    

    def get_arbitrary(json_str, key_1, key_2):
        """
        Takes json string from a DMI climate grid file
        Returns the specified fields
        """
        return json_str[key_1][key_2]