--
-- PostgreSQL database dump
--

-- Started on 2010-03-02 08:36:51 CET

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;


--
-- DROP TABLES FIRST
--

DROP SEQUENCE IF EXISTS flserial_serie_seq CASCADE;
DROP SEQUENCE IF EXISTS flvar_id_seq CASCADE;

DROP TABLE IF EXISTS flserial CASCADE;
DROP TABLE IF EXISTS flareas CASCADE;
DROP TABLE IF EXISTS flfiles CASCADE;
DROP TABLE IF EXISTS flmetadata CASCADE;
DROP TABLE IF EXISTS flmodules CASCADE;
DROP TABLE IF EXISTS flseqs CASCADE;
DROP TABLE IF EXISTS flserial CASCADE;
DROP TABLE IF EXISTS flvar CASCADE;
DROP TABLE IF EXISTS flsettings CASCADE;


--
-- TOC entry 1492 (class 1259 OID 818740)
-- Dependencies: 6
-- Name: flareas; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--
CREATE TABLE flareas (
    descripcion character varying(100) NOT NULL,
    bloqueo boolean NOT NULL,
    idarea character varying(15) NOT NULL
);


--
-- TOC entry 1487 (class 1259 OID 818698)
-- Dependencies: 6
-- Name: flfiles; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE flfiles (
    sha character varying(255),
    idmodulo character varying(15) NOT NULL,
    contenido text,
    bloqueo boolean NOT NULL,
    nombre character varying(255) NOT NULL
);


--
-- TOC entry 1486 (class 1259 OID 818690)
-- Dependencies: 6
-- Name: flmetadata; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE flmetadata (
    seq integer NOT NULL,
    xml text,
    bloqueo boolean,
    tabla character varying(255) NOT NULL
);


--
-- TOC entry 1493 (class 1259 OID 818745)
-- Dependencies: 6
-- Name: flmodules; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE flmodules (
    version character varying(3) NOT NULL,
    icono text,
    descripcion character varying(100) NOT NULL,
    idmodulo character varying(15) NOT NULL,
    bloqueo boolean NOT NULL,
    idarea character varying(15) NOT NULL
);


--
-- TOC entry 1484 (class 1259 OID 818674)
-- Dependencies: 6
-- Name: flseqs; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE flseqs (
    campo character varying(255) NOT NULL,
    seq integer NOT NULL,
    tabla character varying(255) NOT NULL
);


--
-- TOC entry 1488 (class 1259 OID 818706)
-- Dependencies: 6
-- Name: flserial_serie_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE flserial_serie_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


--
-- TOC entry 1791 (class 0 OID 0)
-- Dependencies: 1488
-- Name: flserial_serie_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('flserial_serie_seq', 1, false);


--
-- TOC entry 1490 (class 1259 OID 818725)
-- Dependencies: 1760 6
-- Name: flserial; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--


CREATE TABLE flserial (
    sha character varying(255),
    serie integer DEFAULT nextval('flserial_serie_seq'::regclass) NOT NULL
);


--
-- TOC entry 1485 (class 1259 OID 818682)
-- Dependencies: 6
-- Name: flsettings; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE flsettings (
    flkey character varying(30) NOT NULL,
    valor text
);


--
-- TOC entry 1489 (class 1259 OID 818714)
-- Dependencies: 6
-- Name: flvar_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE flvar_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;


--
-- TOC entry 1792 (class 0 OID 0)
-- Dependencies: 1489
-- Name: flvar_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('flvar_id_seq', 1, false);


--
-- TOC entry 1491 (class 1259 OID 818731)
-- Dependencies: 1761 6
-- Name: flvar; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE flvar (
    id integer DEFAULT nextval('flvar_id_seq'::regclass) NOT NULL,
    idsesion character varying(30) NOT NULL,
    idvar character varying(30) NOT NULL,
    valor text NOT NULL
);


--
-- TOC entry 1784 (class 0 OID 818740)
-- Dependencies: 1492
-- Data for Name: flareas; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO flareas (descripcion, bloqueo, idarea) VALUES ('Sistema', false, 'sys');


--
-- TOC entry 1781 (class 0 OID 818698)
-- Dependencies: 1487
-- Data for Name: flfiles; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- TOC entry 1780 (class 0 OID 818690)
-- Dependencies: 1486
-- Data for Name: flmetadata; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- TOC entry 1785 (class 0 OID 818745)
-- Dependencies: 1493
-- Data for Name: flmodules; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO flmodules (version, icono, descripcion, idmodulo, bloqueo, idarea) VALUES ('0.0', '/* XPM */
static char * configure_xpm[] = {
"32 32 267 2",
"  	c None",
". 	c #B9B9C8",
"+ 	c #BABAC8",
"@ 	c #BABAC9",
"# 	c #CCCCD7",
"$ 	c #B5B5C4",
"% 	c #D5D5DE",
"& 	c #FFFFFF",
"* 	c #F2F2F4",
"= 	c #D1D1DB",
"- 	c #B4B4C3",
"; 	c #B0B0C0",
"> 	c #E1E1E7",
", 	c #F0F0F3",
"'' 	c #BFBFCC",
") 	c #AFAFBF",
"! 	c #AAAABC",
"~ 	c #DFDFE5",
"{ 	c #FAFAFB",
"] 	c #A9A9BB",
"^ 	c #A4A4B7",
"/ 	c #DCDCE3",
"( 	c #F7F7F9",
"_ 	c #AAAABB",
": 	c #9F9FB3",
"< 	c #D9D9E1",
"[ 	c #FEFEFE",
"} 	c #D7D7DF",
"| 	c #9E9EB2",
"1 	c #9999AF",
"2 	c #E7E7EC",
"3 	c #F1F1F4",
"4 	c #9494AA",
"5 	c #9B9BAF",
"6 	c #9393A9",
"7 	c #9A9AAF",
"8 	c #ECECF0",
"9 	c #FDFDFD",
"0 	c #F6F6F8",
"a 	c #8F8FA6",
"b 	c #E6E6EB",
"c 	c #9595AB",
"d 	c #8E8EA5",
"e 	c #A1A1B4",
"f 	c #F8F8FA",
"g 	c #F4F4F6",
"h 	c #BBBBC9",
"i 	c #9D9DB1",
"j 	c #F5F5F7",
"k 	c #E0E0E7",
"l 	c #8888A0",
"m 	c #9C9CB0",
"n 	c #E9E9ED",
"o 	c #EFEFF2",
"p 	c #8989A1",
"q 	c #9191A8",
"r 	c #F1F1F3",
"s 	c #DBDBE2",
"t 	c #8A8AA2",
"u 	c #83839C",
"v 	c #9696AB",
"w 	c #E4E4EA",
"x 	c #EAEAEE",
"y 	c #AEAEBF",
"z 	c #83839D",
"A 	c #7E7E99",
"B 	c #EBEBEF",
"C 	c #85859E",
"D 	c #7D7D98",
"E 	c #9191A7",
"F 	c #EBEBF0",
"G 	c #DEDEE5",
"H 	c #797995",
"I 	c #C3C3CF",
"J 	c #E8E8EC",
"K 	c #CFCFD9",
"L 	c #9898AD",
"M 	c #DADAE1",
"N 	c #D6D6DE",
"O 	c #DFDFE6",
"P 	c #E3E3E9",
"Q 	c #E0E0E6",
"R 	c #D8D8E0",
"S 	c #72728F",
"T 	c #6D6D8C",
"U 	c #DDDDE4",
"V 	c #CECED8",
"W 	c #B2B2C1",
"X 	c #B8B8C6",
"Y 	c #9E9AA7",
"Z 	c #CB8E38",
"` 	c #DA942B",
" .	c #6E6E8C",
"..	c #C5C5D0",
"+.	c #D9D9E0",
"@.	c #DADAE2",
"#.	c #BEBECB",
"$.	c #C2C2CE",
"%.	c #C7C3C7",
"&.	c #D59E4D",
"*.	c #DDA42F",
"=.	c #EDCD37",
"-.	c #D4902B",
";.	c #636383",
">.	c #ACACBD",
",.	c #D6D6DF",
"''.	c #D2D2DB",
").	c #C6C6D2",
"!.	c #CACAD4",
"~.	c #CDC9CC",
"{.	c #CE974A",
"].	c #D99F2D",
"^.	c #EFC229",
"/.	c #E9A91D",
"(.	c #EECE36",
"_.	c #CF8C2A",
":.	c #5D5D7F",
"<.	c #86869F",
"[.	c #A6A6B8",
"}.	c #C5C5D1",
"|.	c #C1C1CE",
"1.	c #BDBDCA",
"2.	c #D0D0DA",
"3.	c #D4D0D2",
"4.	c #CC964C",
"5.	c #D49B2D",
"6.	c #F0C128",
"7.	c #E5840B",
"8.	c #F18809",
"9.	c #ECAC1E",
"0.	c #ECC833",
"a.	c #CA882A",
"b.	c #59597B",
"c.	c #58587A",
"d.	c #656584",
"e.	c #71718E",
"f.	c #787893",
"g.	c #6B6B89",
"h.	c #CECED9",
"i.	c #D9D5D6",
"j.	c #C9944D",
"k.	c #D0962C",
"l.	c #F0BB24",
"m.	c #E88D0F",
"n.	c #FD980D",
"o.	c #FF990D",
"p.	c #F4930E",
"q.	c #EEAF1E",
"r.	c #EAC231",
"s.	c #C5842A",
"t.	c #555578",
"u.	c #9D99A7",
"v.	c #C28D4A",
"w.	c #CC922B",
"x.	c #EDAC1E",
"y.	c #EC9612",
"z.	c #FDA213",
"A.	c #FFA314",
"B.	c #F69D13",
"C.	c #F1B21E",
"D.	c #E8BC2F",
"E.	c #C1802A",
"F.	c #A87438",
"G.	c #C88D2A",
"H.	c #EFAF1E",
"I.	c #EFA015",
"J.	c #FEAC1A",
"K.	c #FFAD1A",
"L.	c #FFAE1A",
"M.	c #F7A718",
"N.	c #F3B51E",
"O.	c #E6B62C",
"P.	c #BC7C29",
"Q.	c #B77829",
"R.	c #DFAB2A",
"S.	c #F7B81F",
"T.	c #FEB720",
"U.	c #FFB820",
"V.	c #F9B11D",
"W.	c #F6B81F",
"X.	c #E4B12A",
"Y.	c #B27529",
"Z.	c #E2AC28",
"`.	c #FFC526",
" +	c #FFC327",
".+	c #FFC126",
"++	c #FFBE24",
"@+	c #FFBD23",
"#+	c #FDBA21",
"$+	c #FEC124",
"%+	c #E2AA27",
"&+	c #AD7129",
"*+	c #E0A525",
"=+	c #FFC929",
"-+	c #FFCD2D",
";+	c #FFCA2B",
">+	c #FFB920",
",+	c #E0A425",
"''+	c #A86D28",
")+	c #DEA023",
"!+	c #FFCD2B",
"~+	c #FFD834",
"{+	c #FFC025",
"]+	c #FFB21D",
"^+	c #FFB01C",
"/+	c #FF9A0E",
"(+	c #FF930A",
"_+	c #FF9209",
":+	c #FFAF1B",
"<+	c #FFB61E",
"[+	c #C88B25",
"}+	c #A76C28",
"|+	c #A36928",
"1+	c #DC9921",
"2+	c #FFD02E",
"3+	c #FFE23A",
"4+	c #FFB01B",
"5+	c #FFA817",
"6+	c #FF9D10",
"7+	c #FF9E11",
"8+	c #FF9108",
"9+	c #EEA81E",
"0+	c #9E6528",
"a+	c #DA941E",
"b+	c #FFD430",
"c+	c #FFA716",
"d+	c #FF8401",
"e+	c #FF9109",
"f+	c #FFA213",
"g+	c #FF8904",
"h+	c #FFA816",
"i+	c #FFB018",
"j+	c #996128",
"k+	c #D88E1C",
"l+	c #FFA113",
"m+	c #FFCC2D",
"n+	c #FF940B",
"o+	c #FF8501",
"p+	c #FF950B",
"q+	c #FF960B",
"r+	c #FFA413",
"s+	c #F9A415",
"t+	c #945D28",
"u+	c #D68619",
"v+	c #FF9C0F",
"w+	c #FFAA17",
"x+	c #FFD733",
"y+	c #FFB61F",
"z+	c #FFAC19",
"A+	c #FFA00F",
"B+	c #D0831A",
"C+	c #8F5927",
"D+	c #D58117",
"E+	c #FF970C",
"F+	c #FF960C",
"G+	c #FF990C",
"H+	c #EA8E12",
"I+	c #965D25",
"J+	c #8A5527",
"K+	c #B56D1C",
"L+	c #F08B0D",
"M+	c #FF9309",
"N+	c #F88F0B",
"O+	c #C47418",
"P+	c #915925",
"Q+	c #8A5627",
"R+	c #855227",
"            . + @ # # @ + .                                     ",
"            $ % & & & & * = $ -                                 ",
"              ; > & & & & & , '' )                               ",
"                ! ~ & & & & & { + ]                             ",
"                  ^ / & & & & & ( _                             ",
"                    : < [ & & & [ } |                           ",
"                      1 2 & & & & 3 :                           ",
"4 5                 6 7 8 9 9 9 9 0 ;                           ",
"a b c             d e > f f f f f g h a                         ",
"i j k a         l m n g g g g g g o $ p                         ",
"q r , s t     u v w , , , , , , , x y z                         ",
"A B 8 8 % C D E ~ 8 8 8 8 8 8 F F G 6                           ",
"H I 2 J J K L M 2 2 2 2 2 2 2 2 2 N H                           ",
"  5 O P P P Q P P P P P P P P P R / i S                         ",
"  T + U ~ ~ ~ ~ ~ ~ ~ ~ ~ G G V W X R Y Z `                     ",
"     ...+.@.@.@.@.@.@.@.@.@.V + #.$.%.&.*.=.-.                  ",
"    ;. .>.# ,.,.,.,.,.N N ''.I ).!.~.{.].^./.(._.                ",
"        :.<.[.. I }.|.- 1.2.V = 3.4.5.6.7.8.9.0.a.              ",
"          b.c.d.e.f.g.;.c.e h.i.j.k.l.m.n.o.p.q.r.s.            ",
"                  t.      t.u.v.w.x.y.z.A.A.A.B.C.D.E.          ",
"                            F.G.H.I.J.K.K.K.K.L.M.N.O.P.        ",
"                            Q.R.S.T.U.U.U.U.U.U.U.V.W.X.Q.      ",
"                              Y.Z.`. + + + + +.+++@+#+$+%+Y.    ",
"                                &+*+=+-+-+-+;+>+U.U.U.U.>+,+&+  ",
"                                  ''+)+!+~+~+{+]+^+/+(+_+:+<+[+}+",
"                                    |+1+2+3+4+K.5+_+6+7+8+L.9+|+",
"                                      0+a+b+c+c+++d+e+f+g+h+i+0+",
"                                        j+k+c+l+m+n+o+p+q+r+s+j+",
"                                          t+u+v+w+x+y+z+v+A+B+t+",
"                                            C+D+E+F+F+E+G+H+I+  ",
"                                              J+K+L+M+N+O+P+Q+  ",
"                                                R+R+R+R+R+      "};
', 'Administración', 'sys', false, 'sys');


--
-- TOC entry 1778 (class 0 OID 818674)
-- Dependencies: 1484
-- Data for Name: flseqs; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- TOC entry 1782 (class 0 OID 818725)
-- Dependencies: 1490
-- Data for Name: flserial; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- TOC entry 1779 (class 0 OID 818682)
-- Dependencies: 1485
-- Data for Name: flsettings; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO flsettings (flkey, valor) VALUES ('sysmodver', '2.3 Build exportado');


--
-- TOC entry 1783 (class 0 OID 818731)
-- Dependencies: 1491
-- Data for Name: flvar; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- TOC entry 1775 (class 2606 OID 818744)
-- Dependencies: 1492 1492
-- Name: flareas_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY flareas
    ADD CONSTRAINT flareas_pkey PRIMARY KEY (idarea);


--
-- TOC entry 1769 (class 2606 OID 818705)
-- Dependencies: 1487 1487
-- Name: flfiles_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY flfiles
    ADD CONSTRAINT flfiles_pkey PRIMARY KEY (nombre);


--
-- TOC entry 1767 (class 2606 OID 818697)
-- Dependencies: 1486 1486
-- Name: flmetadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY flmetadata
    ADD CONSTRAINT flmetadata_pkey PRIMARY KEY (tabla);


--
-- TOC entry 1777 (class 2606 OID 818752)
-- Dependencies: 1493 1493
-- Name: flmodules_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY flmodules
    ADD CONSTRAINT flmodules_pkey PRIMARY KEY (idmodulo);


--
-- TOC entry 1763 (class 2606 OID 818681)
-- Dependencies: 1484 1484
-- Name: flseqs_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY flseqs
    ADD CONSTRAINT flseqs_pkey PRIMARY KEY (tabla);


--
-- TOC entry 1771 (class 2606 OID 818730)
-- Dependencies: 1490 1490
-- Name: flserial_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY flserial
    ADD CONSTRAINT flserial_pkey PRIMARY KEY (serie);


--
-- TOC entry 1765 (class 2606 OID 818689)
-- Dependencies: 1485 1485
-- Name: flsettings_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY flsettings
    ADD CONSTRAINT flsettings_pkey PRIMARY KEY (flkey);


--
-- TOC entry 1773 (class 2606 OID 818739)
-- Dependencies: 1491 1491
-- Name: flvar_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY flvar
    ADD CONSTRAINT flvar_pkey PRIMARY KEY (id);


--
-- TOC entry 1790 (class 0 OID 0)
-- Dependencies: 6
-- Name: public; Type: ACL; Schema: -; Owner: -
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


-- Completed on 2010-03-02 08:36:51 CET

--
-- PostgreSQL database dump complete
--

