namespace PrimesWorld.wit.Exports.demo.primes;

public class CalculExportsImpl : ICalculExports
{
    public static ulong HowManyPrimeInRange(ulong start, ulong end)
    {
        ulong primes = 0;
        for (ulong n = start; n <= end; n++)
        {
            if (IsPrime(n)) primes++;
        }
        return primes;
    }

    private static bool IsPrime(ulong n)
    {
        if (n <= 1) return false;
        ulong limit = (ulong)Math.Sqrt(n);
        for (ulong a = 2; a <= limit; a++)
        {
            if (n % a == 0) return false;
        }
        return true;
    }
}