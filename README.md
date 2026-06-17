# Validation and Extension of Knowledge-Enhanced Electronic ProfilE Review system (KEEPER) in a Network of Real-World Databases

 <img src="https://img.shields.io/badge/Study%20Status-Complete-orange.svg" alt="Study Status: Complete">

## Background
This GitHub repo was established in Fall 2025 to support underlying documentation behind my Oxford doctoral work titled: *Methods for phenotyping complex diseases across a network of real-world databases*. I vivaed and successfully defended in November 2025. My doctoral degree will be awarded in 2026. The information collated in this repo represents a validation effort to support recreation of the [OHDSI](ohdsi.org) community method for large-scale chart adjudication review titled, [Knowledge-Enhanced Electronic ProfilE Review system (KEEPER)](https://ohdsi.github.io/Keeper/). 

### Study Objectives
The goal of KEEPER is to reduce the burden of manual chart review. In the original study, the method reduced the clinical adjudication process time by approximately 50% compared with traditional chart extraction [1]. The approach remains under development within the OHDSI community, including ongoing work to support scalable and reproducible phenotype evaluation. The method is available as an open source R package published on GitHub [2]. To date, validation has been limited to US-based hospital EHR data.

The purpose of this study is to evaluate whether the KEEPER method can be transported to support clinical adjudication in a different healthcare context, specifically UK primary care data. 

The specific objectives of this study include:
-	To describe the availability and completeness of KEEPER’s conceptual elements (e.g. clinical presentation, diagnostics, treatments) in UK primary care data. 
-	To assess the feasibility and limitations of applying the KEEPER method in this setting, including identifying patterns of missing data. 
-	To evaluate whether KEEPER profiles provide sufficient information for clinical adjudication through comparison between clinicians.

The initial study had two components: 1) a validation of one phenotype (Diabetes Type 1) from the original Columbia University paper and 2) extending KEEPER to use a DARWIN-EU(R) phenotype of the same disease area.

### Sub Study: Off-the-Shelf Large-Language Models (LLMs) to Assist Chart Review 

The creators of KEEPER previously evaluated the use of LLMs for case adjudication using models hosted within their local environment, including GPT-3.5 Turbo, GPT-4, Llama-2 and Sheep-Duck-Llama-2 [3]. This work informed the development of the “Using KEEPER with Large Language Models” vignette in the OHDSI KEEPER package. 

In this study, I evaluated the use of commercially available LLMs accessed through the University of Oxford’s institutional licences. These models operate within a different technical and governance environment compared to locally hosted models, with constraints on configuration and access to underlying parameters. While locally hosted open-source LLMs (e.g. frameworks such as Ollama) offer greater control over model configuration, they require dedicated computational infrastructure, secure deployment pipelines, and additional governance when working with RWD. These requirements were not compatible with institutional IT and data governance policies governing the use of RWD at the University of Oxford, including constraints associated with data access and licencing agreements. 

Instead, this analysis uses institutionally licenced LLMs provided by the University of Oxford, which operate within a secure, privacy-preserving infrastructure. This reflects a more realistic use case for applied research settings, where access to configurable local models may be limited or simply beyond a clinical researcher’s skill set. As such, this analysis is not a direct replication of prior work, but a conceptual extension using currently accessible tooling, serving as an early indication of potential pitfalls in their application. The aim is not to establish whether LLMs can replace clinician review, but to assess whether KEEPER profiles provide a sufficient foundation for LLM-based adjudication under these conditions.

## References

1. Ryan P, Ostropolets A, Johnson MS. *Improving the reliability and scale of case validation* [Internet]. OHDSI 2023 Global Symposium; 2023 Oct 20; East Brunswick, NJ. Available from: [OHDSI 2023 Plenary 1 PDF](https://www.ohdsi.org/wp-content/uploads/2023/10/OHDSI2023-Plenary-1.pdf)
2. OHDSI. *An R package to review patient profiles for phenotype validation* [Internet]. [cited 2025 Sep 27]. Available from: [Keeper Documentation](https://ohdsi.github.io/Keeper/index.html)
3. Schuemie MJ, Ostropolets A, Zhuk A, Korsik U, Seo SI, Suchard MA, et al. *Standardized patient profile review using large language models for case adjudication in observational research*. NPJ Digit Med [Internet]. 2025 Jan 9 [cited 2025 Oct 2];8(1):18. Available from: https://doi.org/10.1038/s41746-025-01433-4
