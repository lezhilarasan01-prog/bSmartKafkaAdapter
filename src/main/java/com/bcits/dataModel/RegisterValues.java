package com.bcits.dataModel;


public class RegisterValues
{
private String unit;

private String registerValue;

private String scalar;

private String formattedRegisterObisCode;

private String registerObisCode;

private String attributeId;

private String formattedValue;

public String getUnit ()
{
return unit;
}

public void setUnit (String unit)
{
this.unit = unit;
}

public String getRegisterValue ()
{
return registerValue;
}

public void setRegisterValue (String registerValue)
{
this.registerValue = registerValue;
}

public String getScalar ()
{
return scalar;
}

public void setScalar (String scalar)
{
this.scalar = scalar;
}

public String getFormattedRegisterObisCode ()
{
return formattedRegisterObisCode;
}

public void setFormattedRegisterObisCode (String formattedRegisterObisCode)
{
this.formattedRegisterObisCode = formattedRegisterObisCode;
}

public String getRegisterObisCode ()
{
return registerObisCode;
}

public void setRegisterObisCode (String registerObisCode)
{
this.registerObisCode = registerObisCode;
}

public String getAttributeId ()
{
return attributeId;
}

public void setAttributeId (String attributeId)
{
this.attributeId = attributeId;
}

public String getFormattedValue ()
{
return formattedValue;
}

public void setFormattedValue (String formattedValue)
{
this.formattedValue = formattedValue;
}

@Override
public String toString()
{
return "ClassPojo [unit = "+unit+", registerValue = "+registerValue+", scalar = "+scalar+", formattedRegisterObisCode = "+formattedRegisterObisCode+", registerObisCode = "+registerObisCode+", attributeId = "+attributeId+", formattedValue = "+formattedValue+"]";
}
}

