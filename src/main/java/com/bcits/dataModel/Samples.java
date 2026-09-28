package com.bcits.dataModel;

public class Samples
{
    private RegisterValues[] registerValues;

    private String time;

    public RegisterValues[] getRegisterValues ()
    {
        return registerValues;
    }

    public void setRegisterValues (RegisterValues[] registerValues)
    {
        this.registerValues = registerValues;
    }

    public String getTime ()
    {
        return time;
    }

    public void setTime (String time)
    {
        this.time = time;
    }

    @Override
    public String toString()
    {
        return "ClassPojo [registerValues = "+registerValues+", time = "+time+"]";
    }
}
		