
  <div style="padding:10px;display:flex;align-items:end">
    <div style="display:inline-block">
      <a href="https://srccircumflex.github.io/wsqlite3">
        <img src="https://raw.githubusercontent.com/srccircumflex/wsqlite3/main/docs/logo.png" alt="logo" style="display:inline-block">
      </a>
    </div>
    <div style="display:inline-block">
      <h1>
        <span style="color:#266ed9">W</span><span style="border-bottom:1px solid #266ed9"><span style="color:#266ed9">S</span><span>QL</span><span>ite3</span></span>
        <span>v0.7</span>
      </h1>
    </div>
  </div>

```commandline
python -m pip install wsqlite3 --upgrade
```


  <p>
    <b>WSQLite3</b> (<cite>WebSocket SQLite3</cite>) is a simple and lightweight solution to manage
    access from multiple processes (or threads) to <b>SQLite3</b> databases.
  </p>

  <span><b><cite>What it is and what it is not:</cite></b></span>
  <div style="padding-left:30px;border-left:3px solid #266ed9">
    <p>
      The focus of <b>WSQLite3</b> is on simple local session management of multiple instances of a program or
      compatibilization of different programs. <b>WSQLite3</b> is not intended as a database in a wide-ranging system for
      multiple end users. For this, other advanced database systems with client servers should be used.
    </p>
    <p>
      <mark>
        <b>WSQLite3</b> in the basic version is designed for <cite>friendly</cite> connections,
        an SSL is NOT implemented and there is NO permission handling
        (Caution: <b>WSQLite3</b> also allows remote code execution).
      </mark>
    </p>
  </div>
  <span><b><cite>How it works and properties:</cite></b></span>
  <div style="padding-left:30px;border-left:3px solid #266ed9">
    <p>
      <b>WSQLite3</b> manages connections to <b>SQLite3</b> databases for multiple clients per server.
      The communication via the <b>WebSocket</b> protocol with <b>JSON</b> data provides simple and high compatibility
      between different programs and programming languages.
      The focus of the project is on the <b>WSQLite3 service</b>, which is intended as an independent process.
      For complex projects, a separate implementation of the client side should be embedded.
    </p>
  </div>








<a href="https://pypi.org/project/wsqlite3" target="_blank" style="position: absolute;top: 22px; right: 62px;color: #db54d9; z-index:100;">
<img src="https://pypi.org/static/images/logo-small.8998e9d1.svg" alt="pypi.org/wsqlite3" style="height: 24px;">
</a>




<hr>
<p>
  <a href="https://srccircumflex.github.io/wsqlite3"><b> &#9654; &nbsp; Documentation</b></a>
</p> 



