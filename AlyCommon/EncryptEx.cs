using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace AlyCommon
{
    /// <summary>
    /// 表示加密拓展类型
    /// </summary>
    public static class EncryptEx
    {
        /// <summary>
        /// 根据指定的算法,散列加密字符串.
        /// </summary>
        /// <param name="plainText">需要加密的字符串.</param>
        /// <param name="algorithm">算法, 例如: MD5.Create()</param>
        /// <remarks>注意:此为单向加密,无法解密!</remarks>
        /// <returns>返回加密后的字符串.</returns>
        /// <exception cref="System.ArgumentNullException">参数为空时,导致异常.</exception>
        public static string HashEncrypt(this string plainText, HashAlgorithm algorithm)
        {
            // Check arguments.
            if (plainText == null || plainText.Length == 0) { throw new ArgumentNullException("plainText"); }
            if (algorithm == null) { throw new ArgumentNullException("algorithm"); }

            byte[] unicodeBytes = Encoding.Unicode.GetBytes(plainText);
            byte[] hashBytes = algorithm.ComputeHash(unicodeBytes);
            return hashBytes.Aggregate(string.Empty, (a, s) => a += s.ToString("x2"));
        }

        /// <summary>
        /// 根据指定的算法,验证指定字符串与指定散列字符串是否相等.
        /// </summary>
        /// <param name="input">需要散列加密的字符串</param>
        /// <param name="hash">散列字符串</param>
        /// <param name="algorithm">散发,如: MD5.Create()</param>
        /// <returns>一个<see cref="bool"/>值,标示指定字符串与散列字符串是否相等.</returns>
        public static bool HashVerify(this string input, string hash, HashAlgorithm algorithm)
        {
            string imputHash = input.HashEncrypt(algorithm);
            StringComparer comparer = StringComparer.OrdinalIgnoreCase;
            return comparer.Compare(imputHash, hash) == 0 ? true : false;
        }

        /// <summary>
        /// AES算法加密
        /// </summary>
        /// <param name="plainText">需要加密的字符串</param>
        /// <param name="key">密匙</param>
        /// <returns>加密后的Base64字符串</returns>
        /// <exception cref="System.ArgumentNullException">参数为空时,导致异常.</exception>
        public static string AesEncrypt(this string plainText)
        {
            // Check arguments.
            if (plainText == null || plainText.Length < 1)
                throw new ArgumentNullException("plainText");

            // Return the encrypted base 64 string from the memory stream.
            return plainText.AesEncrypt("allyn@live.cn");

        }

        /// <summary>
        /// AES算法解密
        /// </summary>
        /// <param name="cipherText">需要解密的Base64字符串</param>
        /// <param name="key">密匙</param>
        /// <returns>解密后的字符串</returns>
        /// <remarks>如果解密的字符串不是Base64编码格式,会报错.</remarks>
        /// <exception cref="System.ArgumentNullException">参数为空时,导致异常.</exception>
        public static string AesDecrypt(this string cipherText)
        {
            // Check arguments.
            if (cipherText == null || cipherText.Length == 0)
                throw new ArgumentNullException("cipherText");

            // Read the decrypted bytes from the decrypting stream and place them in expression string.
            return cipherText.AesDecrypt("allyn@live.cn");
        }

        /// <summary>
        /// AES算法加密
        /// </summary>
        /// <param name="plainText">需要加密的字符串</param>
        /// <param name="key">密匙</param>
        /// <returns>加密后的Base64字符串</returns>
        /// <exception cref="System.ArgumentNullException">参数为空时,导致异常.</exception>
        public static string AesEncrypt(this string plainText, string key)
        {
            // Check arguments.
            if (plainText == null || plainText.Length < 1)
                throw new ArgumentNullException("plainText");
            if (key == null || key.Length < 1)
                throw new ArgumentNullException("Key");

            // Return the encrypted base 64 string from the memory stream.
            return plainText.AesEncrypt(key, "www.allyn.com.cn");

        }

        /// <summary>
        /// AES算法解密
        /// </summary>
        /// <param name="cipherText">需要解密的Base64字符串</param>
        /// <param name="key">密匙</param>
        /// <returns>解密后的字符串</returns>
        /// <remarks>如果解密的字符串不是Base64编码格式,会报错.</remarks>
        /// <exception cref="System.ArgumentNullException">参数为空时,导致异常.</exception>
        public static string AesDecrypt(this string cipherText, string key)
        {
            // Check arguments.
            if (cipherText == null || cipherText.Length == 0)
                throw new ArgumentNullException("cipherText");
            if (key == null || key.Length < 1)
                throw new ArgumentNullException("Key");

            // Read the decrypted bytes from the decrypting stream and place them in expression string.
            return cipherText.AesDecrypt(key, "www.allyn.com.cn");
        }

        /// <summary>
        /// AES算法加密
        /// </summary>
        /// <param name="plainText">需要加密的字符串</param>
        /// <param name="key">密匙</param>
        /// <param name="iv">向量</param>
        /// <returns>加密后的Base64字符串</returns>
        /// <exception cref="System.ArgumentNullException">参数为空时,导致异常.</exception>
        public static string AesEncrypt(this string plainText, string key, string iv)
        {
            // Check arguments.
            if (plainText == null || plainText.Length < 1)
                throw new ArgumentNullException("plainText");
            if (key == null || key.Length < 1)
                throw new ArgumentNullException("Key");
            if (iv == null || iv.Length < 1)
                throw new ArgumentNullException("iv");

            // Create an Rijndael object with the specified key and IV.
            using (Rijndael rijAlg = Rijndael.Create())
            {
                rijAlg.Key = SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(key));
                rijAlg.IV = MD5.Create().ComputeHash(Encoding.UTF8.GetBytes(iv));

                // Create expression decrytor to perform the stream transform.
                ICryptoTransform encryptor = rijAlg.CreateEncryptor(rijAlg.Key, rijAlg.IV);

                // Create the streams used for encryption.
                using (MemoryStream msEncrypt = new MemoryStream())
                {
                    using (CryptoStream csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write))
                    {
                        using (StreamWriter swEncrypt = new StreamWriter(csEncrypt, Encoding.UTF8))
                        {
                            //Write all hashBytes to the stream.
                            swEncrypt.Write(plainText);
                        }
                    }
                    // Return the encrypted base 64 string from the memory stream.
                    return Convert.ToBase64String(msEncrypt.ToArray());
                }
            }
        }

        /// <summary>
        /// AES算法解密
        /// </summary>
        /// <param name="cipherText">需要解密的Base64字符串</param>
        /// <param name="key">密匙</param>
        /// <param name="iv">向量</param>
        /// <returns>解密后的字符串</returns>
        /// <remarks>如果解密的字符串不是Base64编码格式,会报错.</remarks>
        /// <exception cref="System.ArgumentNullException">参数为空时,导致异常.</exception>
        public static string AesDecrypt(this string cipherText, string key, string iv)
        {
            // Check arguments.
            if (cipherText == null || cipherText.Length == 0)
                throw new ArgumentNullException("cipherText");
            if (key == null || key.Length < 1)
                throw new ArgumentNullException("Key");
            if (iv == null || iv.Length < 1)
                throw new ArgumentNullException("iv");

            // Create an Rijndael object with the specified key and IV.
            using (Rijndael rijAlg = Rijndael.Create())
            {
                rijAlg.Key = SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(key));
                rijAlg.IV = MD5.Create().ComputeHash(Encoding.UTF8.GetBytes(iv));

                // Create expression decrytor to perform the stream transform.
                ICryptoTransform decryptor = rijAlg.CreateDecryptor(rijAlg.Key, rijAlg.IV);

                // Create the streams used for decryption.
                using (MemoryStream msDecrypt = new MemoryStream(Convert.FromBase64String(cipherText)))
                {
                    using (CryptoStream csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read))
                    {
                        using (StreamReader srDecrypt = new StreamReader(csDecrypt, Encoding.UTF8))
                        {
                            // Read the decrypted bytes from the decrypting stream and place them in expression string.
                            return srDecrypt.ReadToEnd();
                        }
                    }
                }
            }
        }
    }
}
